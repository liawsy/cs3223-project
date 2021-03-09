/** 
 * Performs external sort on the file
 */
package qp.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class ExternalSort extends Operator {
    Operator base; // base table to sort
    Schema schema; // base table schema
    int tuplesPerBatch; // number of tuples per batch
    int numBuffer; // number of buffer available
    ArrayList<Integer> attributeIndices = new ArrayList<>(); // index of attributes to sort on

    public ExternalSort(Operator base, ArrayList<Attribute> attributeList, int numBuffer) {
        super(OpType.SORT);

        this.base = base;
        this.schema = base.schema;
        this.numBuffer = numBuffer;

        for (int i = 0; i < attributeList.size(); i++) {
            Attribute attribute = attributeList.get(i);
            attributeIndices.add(schema.indexOf(attribute));
        }
    }

    /**
     * opens connection to the base operator
     */
    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        // find number of tuples per batch
        int tupleSize = schema.getTupleSize();
        tuplesPerBatch = Batch.getPageSize() / tupleSize;

        int numSortedRun = createSortedRuns();

        mergeSortedRuns(numSortedRun);

        return true;
    }

    public int createSortedRuns() {

        Batch inputBatch = base.next();
        int numSortedRun = 0;

        // while the table is not empty
        while (inputBatch != null) {

            ArrayList<Tuple> tuplesInSortedRun = new ArrayList<Tuple>();

            // 1 buffer = 1 batch = 1 page
            // read in as many batches as number of buffers
            for (int i = 0; i < numBuffer; i++) {
                // adds all tuples from input batch to sorted run
                tuplesInSortedRun.addAll(inputBatch.getTuples());

                Batch nextBatch = base.next();
                if (nextBatch != null) {
                    inputBatch = nextBatch;
                }
            }
            numSortedRun++;

            // sort tuples
            tuplesInSortedRun.sort(this::tupleComparator);

            // generating of sorted runs => considered as pass 0
            writeTuplesToFile(tuplesInSortedRun, numSortedRun, 0);

            inputBatch = base.next();
        }
        return numSortedRun;
    }

    public void mergeSortedRuns(int numSortedRun) {
        int numInputBuffer = numBuffer - 1;
        int numRunsToMerge = numSortedRun;
        int passId = 0;

        while (numRunsToMerge > 1) {
            // k way merge
            int outputRunId = 0;
            for (int start = 0; start < numRunsToMerge; start = start + numInputBuffer) {
                int end = Math.min(start + numInputBuffer, numRunsToMerge) - 1;
                mergeRunsBetween(start, end, passId, numInputBuffer, outputRunId);
                outputRunId++;
            }
            numRunsToMerge = (int) Math.ceil(numRunsToMerge / (double) numInputBuffer);
            passId++;
            clearFiles(passId);
        }
    }

    public void clearFiles(int passId) {
        File directory = new File("../operators");
        for (File f : directory.listFiles()) {
            // keeps the last sorted file and all java files
            // if (!f.getName().startsWith("pass_" + passId) && !f.getName().endsWith(".java")) {
            if (!f.getName().startsWith("pass_0") && !f.getName().endsWith(".class") && !f.getName().endsWith(".java")) {
                f.delete();
            }
        }
    }

    public void mergeRunsBetween(int start, int end, int passId, int numInputBuffer, int outputRunId) {
        // number of runs to merge
        int numRuns = end - start + 1;
        // tracks input streams of Tuple objects from file
        ObjectInputStream[] inputStreams = new ObjectInputStream[numRuns];
        // tracks if stream has ended
        boolean[] inputEos = new boolean[numRuns];
        // tracks buffer pages read in from input stream
        Batch[] inputBatches = new Batch[numRuns];

        // initial population of buffers(batches)
        for (int i = start; i <= end; i++) {
            int arrIndex = i % numRuns;
            // 1. set up ObjectInputStreams to read from file
            try {
                FileInputStream fileIn = new FileInputStream("pass_" + passId + "_sorted_run_" + i);
                ObjectInputStream inStream = new ObjectInputStream(fileIn);
                inputStreams[arrIndex] = inStream;
                inputEos[arrIndex] = false;
                
                // 2. put as many tuples as possible into batch
                Batch inputBatch = new Batch(tuplesPerBatch);
                while (!inputBatch.isFull()) {
                    inputBatch.add((Tuple) inStream.readObject());
                }
                inputBatches[arrIndex] = inputBatch;
            
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        Batch outputBatch = new Batch(tuplesPerBatch);
        int outputPassId = passId + 1;
        // while there is still a stream that has not reached eos
        while (!reachedEndOfStreams(inputEos)) {
            // 3. compare across the first tuple of all batch

            Tuple minTuple = null;
            int minPtr = 0;   // points to the first non empty tuple
            int minBatch = 0; // points to the current batch that minTuple belongs to
            
            // sets the first tuple of the first non empty batch as minTuple
            for (int i = 0; i < numRuns; i++) {
                if (!inputBatches[i].isEmpty()) {
                    minTuple = inputBatches[i].get(0); 
                    minPtr = i;
                    minBatch = i; 
                    break;
                }
            }

            // looks through the rest of the batches
            for (int i = minPtr + 1; i < numRuns; i++) {
                Batch currBatch = inputBatches[i];
                
                // cannot get from buffers that are empty
                if (currBatch.isEmpty()) {
                    continue;
                }

                Tuple currTuple = currBatch.get(0);
                
                if (tupleComparator(currTuple, minTuple) < 0) {
                    minTuple = currTuple;
                    minBatch = i;
                }
            }
            
            // 4. add smallest to output buffer
            outputBatch.add(minTuple);

            if (outputBatch.isFull()) {
                 // if buffer is full, write to file then clear the buffer
                ArrayList<Tuple> TuplesToWrite = outputBatch.getTuples();
                
                File tempFile = new File("pass_" + outputPassId + "_sorted_run_" + outputRunId);
                boolean exists = tempFile.exists();
                if (exists) {
                    writeTuplesToExistingFile(TuplesToWrite, outputRunId, outputPassId);
                } else {
                    writeTuplesToFile(TuplesToWrite, outputRunId, outputPassId);
                }
                outputBatch.clear();
            }
            

            // 5. remove min Tuple and read a new Tuple into batch
            try {    
                inputBatches[minBatch].remove(0);
                ObjectInputStream stream = inputStreams[minBatch];
                Tuple newTuple = (Tuple) stream.readObject();
                inputBatches[minBatch].add(newTuple);
                
            } catch (Exception e) {
                
                try {
                    inputStreams[minBatch].close();
                    inputEos[minBatch] = true;
                } catch (IOException io) {
                    continue;
                }
                
                e.printStackTrace();
                // inputStreams[minBatch].close();
                continue;
            }
        }
    }

    /**
     * 
     * @param inputEos
     * @return true if has reached end of streams for all streams, false if there are streams that have not ended
     */
    private boolean reachedEndOfStreams(boolean[] inputEos) {
        boolean result = true;
        for (boolean x : inputEos) {
            result = result && x;
        }
        return result;
    }

    /**
     * 
     * @param t1 is the first tuple
     * @param t2 is the second tuple
     * @return -1 if t1 comes before t2, 0 if they are equal, 1 if t2 comes before t1
     */
    private int tupleComparator(Tuple t1, Tuple t2) {
        int result = 0;
        for (int i = 0; i < attributeIndices.size(); i++) {
            result = Tuple.compareTuples(t1, t2, attributeIndices.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }

    private static void writeTuplesToExistingFile(ArrayList<Tuple> sortedTuples, int sortedRunId, int passId) {
        try {
            // add to file
            System.out.println("\npass " + passId + " run " + sortedRunId);
            FileOutputStream fileOut = new FileOutputStream("pass_" + passId + "_sorted_run_" + sortedRunId, true);
            ObjectOutputStream objectOut = new AppendableObjectOutputStream(fileOut);
            for (Tuple Tuple : sortedTuples) {
                System.out.println("appending to file: ");
                System.out.println(Tuple.toString());
                
                objectOut.writeObject(Tuple);
            }
            objectOut.close();
            fileOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeTuplesToFile(ArrayList<Tuple> sortedTuples, int sortedRunId, int passId) {
        try {
            // add to file
            FileOutputStream fileOut = new FileOutputStream("pass_" + passId + "_sorted_run_" + sortedRunId);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            for (Tuple tuple : sortedTuples) {
                objectOut.writeObject(tuple);
            }
            objectOut.close();
            fileOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Batch next() {
        // return next batch of sorted tuple
        
        return new Batch(tuplesPerBatch);
    }

    public boolean close() {
        super.close();
        return true;
    }

    static class AppendableObjectOutputStream extends ObjectOutputStream {
        public AppendableObjectOutputStream(FileOutputStream out) throws IOException {
          super(out);
        }
      
        @Override
        protected void writeStreamHeader() throws IOException {
          reset();
        }
    }
   
}