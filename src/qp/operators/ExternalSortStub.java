package qp.operators;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleInRun;

/**
 * Applies external sort on a given relation.
 */
public class ExternalSortStub extends Operator {
    // The base operator (i.e., the unsorted relation).
    private final Operator base;
    // A random UUID to avoid conflicts between different instances of Sort operators.
    private final String uuid = UUID.randomUUID().toString();
    // The number of buffer pages available.
    private final int numOfBuffers;
    // The index of the attribute to sort based on.
    private final Vector<Integer> sortKeyIndices = new Vector<>();
    // The number of tuples per batch.
    private final int batchSize;
    // The input stream from which we read the sorted result.
    private ObjectInputStream sortedStream;
    // Records whether we have reached out-of-stream for the sorted result.
    private boolean eos = false;

    /**
     * Creates a new sort operator.
     *
     * @param base         is the base operator.
     * @param numOfBuffers is the number of buffers (in pages) available.
     */
    public ExternalSortStub(Operator base, Vector attrList, int numOfBuffers) {
        super(OpType.SORT);
        this.schema = base.schema;

        this.base = base;
        this.numOfBuffers = numOfBuffers;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();

        for (int i = 0; i < attrList.size(); i++) {
            Attribute attribute = (Attribute) attrList.elementAt(i);
            sortKeyIndices.add(schema.indexOf(attribute));
        }
    }

    /**
     * Opens the operator to prepare all the necessary resources. In the current implementation,
     * the {@link Sort} operator is not iterator-based. We would materialize it by finishing the
     * whole sorting when we open it.
     *
     * @return true if the operator is open successfully.
     */
    @Override
    public boolean open() {
        // Makes sure the base operator can open successfully first.
        if (!base.open()) {
            return false;
        }

        // Phase 1: generate sorted runs using in-memory sorting algorithms.
        int numOfRuns = generateSortedRuns();
        // Phase 2: merge sorted runs together (could be in multiple passes).
        return mergeRuns(numOfRuns, 1) == 1;
    }

    /**
     * Generates sorted runs and stores them to disk. Currently, we utilize all buffer pages available
     * and thus minimizes the number of runs generated.
     *
     * @return the number of sorted runs generated.
     */
    private int generateSortedRuns() {
        // Reads in the next page from base operator.
        Batch inBatch = base.next();

        int numOfRuns = 0;
        while (inBatch != null) {
            // Stores the tuples that will be in the current run.
            Vector<Tuple> tuplesInRun = new Vector<>();

            // Reads in as many tuples as possible (until either there is no more tuples or reaches buffer limit).
            for (int i = 0; i < numOfBuffers && inBatch != null; i++) {
                tuplesInRun.addAll(inBatch.getTuples());
                // Do NOT read a new page if the current page is the last page of the current sorted run
                // (because we do not have any more memory buffer).
                if (i != numOfBuffers - 1) {
                    inBatch = base.next();
                }
            }

            // Sorts the tuples using in-memory sorting algorithms.
            tuplesInRun.sort(this::compareTuples);

            // Stores the sorted result into disk (phase 1 is the 0th pass).
            String fileName = getSortedRunFileName(0, numOfRuns);
            try {
                ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
                // TODO: do we need to write each batch to a single file?
                for (Tuple tuple : tuplesInRun) {
                    stream.writeObject(tuple);
                }
                stream.close();
            } catch (IOException e) {
                System.err.printf("Sort: unable to write sortedRun with ID=%d due to %s\n", numOfRuns, e.toString());
                System.exit(1);
            }

            // Reads in another page and prepares for the next iteration.
            inBatch = base.next();
            numOfRuns++;
        }

        return numOfRuns;
    }

    /**
     * Merges a given number of sorted runs in a manner similar to merge-sort. Here we use all available
     * buffers to minimize the number of passes.
     *
     * @param numOfRuns is the number of sorted runs to be merged.
     * @param passID    is the ID of the current pass.
     * @return the number of sorted runs after one round of merge.
     */
    private int mergeRuns(int numOfRuns, int passID) {
        // Exits if there is no more than 1 run (which means there is no need to merge anymore).
        if (numOfRuns <= 1) {
            try {
                String fileName = getSortedRunFileName(passID - 1, numOfRuns - 1);
                sortedStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException e) {
                System.err.printf("Sort: cannot create sortedStream due to %s\n", e.toString());
            }
            return numOfRuns;
        }

        // Uses (numOfBuffers - 1) as input buffers, and the left one as output buffer.
        int numOfOutputRuns = 0;
        for (int startRunID = 0; startRunID < numOfRuns; startRunID = startRunID + numOfBuffers - 1) {
            int endRunID = Math.min(startRunID + numOfBuffers - 1, numOfRuns);
            try {
                mergeRunsBetween(startRunID, endRunID, passID, numOfOutputRuns);
            } catch (IOException e) {
                System.err.printf("Sort: cannot mergeRuns on passID=%d for [%d, %d) due to %s\n", passID, startRunID, endRunID, e.toString());
                System.exit(1);
            } catch (ClassNotFoundException e) {
                System.err.printf("Sort: class not found on passID=%d for [%d, %d) due to %s\n", passID, startRunID, endRunID, e.toString());
                System.exit(1);
            }
            numOfOutputRuns++;
        }

        // Continues to the next round using a recursive call.
        return mergeRuns(numOfOutputRuns, passID + 1);
    }

    /**
     * Merges the sorted runs in the range of [startRunID, endRunID). Since we have a fix number of buffer
     * pages, we assume endRunID - startRunID <= numOfBuffers - 1.
     *
     * @param startRunID is the sorted run ID of the lower bound (inclusive).
     * @param endRunID   is the sorted run ID of the upper bound (exclusive).
     * @param passID     is the ID of the current pass.
     * @param outID      is the sorted run ID of the output.
     * @implNote we effectively implement a k-way merge sort here.
     */
    private void mergeRunsBetween(int startRunID, int endRunID, int passID, int outID) throws IOException, ClassNotFoundException {
        // Each input sorted run has 1 buffer page.
        Batch[] inBatches = new Batch[endRunID - startRunID];
        // Records whether a certain input sorted run has reached its end-of-stream.
        boolean[] inEos = new boolean[endRunID - startRunID];
        // Each input sorted run has one stream to read from.
        ObjectInputStream[] inStreams = new ObjectInputStream[endRunID - startRunID];
        for (int i = startRunID; i < endRunID; i++) {
            String inputFileName = getSortedRunFileName(passID - 1, i);
            ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(inputFileName));
            inStreams[i - startRunID] = inStream;

            // Fills in the data from an input run.
            Batch inBatch = new Batch(batchSize);
            while (!inBatch.isFull()) {
                try {
                    Tuple data = (Tuple) inStream.readObject();
                    inBatch.add(data);
                } catch (EOFException eof) {
                    break;
                }
            }
            inBatches[i - startRunID] = inBatch;
        }

        // A min-heap used later for k-way merge (representing the 1 page used for output buffer).
        PriorityQueue<TupleInRun> outHeap = new PriorityQueue<>(batchSize, (o1, o2) -> compareTuples(o1.tuple, o2.tuple));
        // The stream for output buffer.
        String outputFileName = getSortedRunFileName(passID, outID);
        ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(outputFileName));

        // Inserts the 1st element (i.e., the smallest element) from each input buffer into the output heap.
        for (int i = 0; i < endRunID - startRunID; i++) {
            Batch inBatch = inBatches[i];
            if (inBatch == null || inBatch.isEmpty()) {
                inEos[i] = true;
                continue;
            }
            Tuple current = inBatch.elementAt(0);
            outHeap.add(new TupleInRun(current, i, 0));
        }

        // Continues until the output heap is empty (which means we have sorted and outputted everything).
        while (!outHeap.isEmpty()) {
            // Extracts and writes out the root of the output heap (which is the smallest element among all input runs).
            TupleInRun outTuple = outHeap.poll();
            outStream.writeObject(outTuple.tuple);

            // Attempts to retrieve the next element from the same input buffer.
            int nextBatchID = outTuple.runID;
            int nextIndex = outTuple.tupleID + 1;
            // Reads in the next page from the same input stream if that input buffer has been exhausted.
            if (nextIndex == batchSize) {
                Batch inBatch = new Batch(batchSize);
                while (!inBatch.isFull()) {
                    try {
                        Tuple data = (Tuple) inStreams[nextBatchID].readObject();
                        inBatch.add(data);
                    } catch (EOFException eof) {
                        break;
                    }
                }
                inBatches[nextBatchID] = inBatch;

                // Resets the index for that input buffer to be 0.
                nextIndex = 0;
            }

            // Inserts the next element into the output heap if that input buffer is not empty.
            Batch inBatch = inBatches[nextBatchID];
            if (inBatch == null || inBatch.size() <= nextIndex) {
                inEos[nextBatchID] = true;
                continue;
            }
            Tuple nextTuple = inBatch.elementAt(nextIndex);
            outHeap.add(new TupleInRun(nextTuple, nextBatchID, nextIndex));
        }

        // Closes the resources used.
        for (ObjectInputStream inStream : inStreams) {
            inStream.close();
        }
        outStream.close();
    }

    /**
     * Compares two tuples based on the sort attribute.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return an integer indicating the comparision result, compatible with the {@link java.util.Comparator} interface.
     */
    private int compareTuples(Tuple tuple1, Tuple tuple2) {
        for (int sortKeyIndex: sortKeyIndices) {
            int result = Tuple.compareTuples(tuple1, tuple2, sortKeyIndex);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Provides the file name of a generated sorted run based on its run ID.
     *
     * @param passID is the ID of the current pass.
     * @param runID  is the ID of the sorted run.
     */
    private String getSortedRunFileName(int passID, int runID) {
        return "Sort-run-" + uuid + "-" + passID + "-" + runID;
    }

    /**
     * @return the next sorted page of tuples from the sorting result.
     */
    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        Batch outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            try {
                Tuple data = (Tuple) sortedStream.readObject();
                outBatch.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.printf("Sort: class not found for reading from sortedStream due to %s\n", cnf.toString());
                System.exit(1);
            } catch (EOFException EOF) {
                // Sends the incomplete page and close in the next call.
                eos = true;
                return outBatch;
            } catch (IOException e) {
                System.err.printf("Sort: error reading from sortedStream due to %s\n", e.toString());
                System.exit(1);
            }
        }
        return outBatch;
    }

    /**
     * Closes the operator by gracefully closing the resources opened.
     * TODO: delete generated files when closing.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        // Calls the close method in super-class for compatibility.
        super.close();

        // Closes the sorted stream previously opened.
        try {
            sortedStream.close();
        } catch (IOException e) {
            System.err.printf("Sort: unable to close sortedStream due to %s\n", e.toString());
            return false;
        }
        return true;
    }
}
