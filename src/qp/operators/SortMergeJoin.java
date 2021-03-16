package qp.operators;

import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    ExternalSort leftSorted;
    ExternalSort rightSorted;

    int batchSize;                    // Number of tuples per out batch
    ArrayList<Integer> leftIndices;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndices;  // Indices of the join attributes in right table
    Batch outBatch;                   // Buffer page for output
    Batch leftBatch;                  // Buffer page for left input stream
    Batch rightBatch;                 // Buffer page for right input stream

    int leftPtr;                      // Index pointer for left side buffer
    int rightPtr;                     // Index pointer for right side buffer
    boolean eosl;                     // Whether end of stream (left table) is reached
    boolean eosr;                     // Whether end of stream (right table) is reached
    Tuple leftTuple;                  // current tuple from left table
    Tuple rightTuple;                 // current tuple from right table
    Tuple peekRightTuple;              // next tuple from right table
    ArrayList<Tuple> rightPartition;  // collection of duplicates on right table
    int rightPartitionPtr;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        // batch size = num of tuples per batch
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        ArrayList<Attribute> leftAttrs = new ArrayList<>();
        ArrayList<Attribute> rightAttrs = new ArrayList<>();

        /** find indices attributes of join conditions **/
        leftIndices = new ArrayList<>();
        rightIndices = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftAttrs.add(leftattr);
            rightAttrs.add(rightattr);
            leftIndices.add(left.getSchema().indexOf(leftattr));
            rightIndices.add(right.getSchema().indexOf(rightattr));
        }

        leftSorted = new ExternalSort(left, leftAttrs, numBuff);
        rightSorted = new ExternalSort(right, rightAttrs, numBuff);

        leftSorted.setPrefix("SMJ-left_");
        rightSorted.setPrefix("SMJ-right_");

        // open left and right sorted tables
        leftSorted.open();
        rightSorted.open();

        // initialize values
        leftPtr = 0;
        rightPtr = 0;
        eosl = false;
        eosr = false;
        leftTuple = null;
        rightTuple = null;
        peekRightTuple = null;
        leftBatch = null;
        rightBatch = null;
        rightPartition = new ArrayList<Tuple>();
        rightPartitionPtr = 0;

        return true;
    }

    public Batch next() {
        // terminate if either file reaches end (no more tuples to merge)
        if (eosl || eosr) {
            close();
            return null;
        }

        // 1. if no batch read yet, read one batch in from both sides
        if (leftBatch == null) {
            leftBatch = leftSorted.next();
            if (leftBatch == null) {
                eosl = true;
                return null;
            }
            // try reading in one tuple on the left table
            leftTuple = getNextLeftTuple();
            if (leftTuple == null) {
                eosl = true;
                return null;
            }
            leftPtr = 0;
        }

        if (rightBatch == null) {
            rightBatch = rightSorted.next();
            if (rightBatch == null) {
                eosr = true;
                return null;
            }
            // create the entire right partition
            createRightPartition();
            if (rightPartition.isEmpty()) {
                eosr = true;
                return null;
            }
            rightPtr = 0;
            rightPartitionPtr = 0;
            rightTuple = rightPartition.get(rightPartitionPtr);
        }

        Batch outBatch = new Batch(batchSize);
        
        while (!outBatch.isFull()) {
            int result = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
            if (result == 0) {
                outBatch.add(leftTuple.joinWith(rightTuple));

                // if there are more tuples in the right partition
                if (rightPartitionPtr < rightPartition.size() - 1) {
                    rightPartitionPtr++;
                    rightTuple = rightPartition.get(rightPartitionPtr);
                } else {
                    // move left pointer forward
                    Tuple peekLeftTuple = getNextLeftTuple();
                    if (peekLeftTuple == null) {
                        eosl = true;
                        break;
                    }
                    int compareLeftTuples = Tuple.compareTuples(peekLeftTuple, leftTuple, leftIndices, leftIndices);
                    // if peekLeft is bigger than current left tuple, create a new partition
                    if (compareLeftTuples > 0) {
                        createRightPartition();
                        if (rightPartition.isEmpty()) {
                            eosr = true;
                            break;
                        }
                    }
                    leftTuple = peekLeftTuple;
                    rightPartitionPtr = 0;
                    rightTuple = rightPartition.get(rightPartitionPtr);
                }

            } else if (result < 0) {
                leftTuple = getNextLeftTuple();
                if (leftTuple == null) {
                    eosl = true;
                    return outBatch;
                }
            } else { // result > 0
                createRightPartition();
                if (rightPartition.size() == 0) {
                    eosr = true;
                    break;
                }
                rightPartitionPtr = 0;
                rightTuple = rightPartition.get(rightPartitionPtr);
            }
        }
        return outBatch;
    }

    private Tuple getNextLeftTuple() {
        if (leftBatch == null) {
            eosl = true;
            return null;
        } else if (leftPtr == leftBatch.size()) {
            leftBatch = leftSorted.next();
            leftPtr = 0;
        }

        if (leftBatch == null || leftBatch.size() <= leftPtr) {
            eosl = true;
            return null;
        }

        Tuple nextLeftTuple = leftBatch.get(leftPtr);
        leftPtr++;
        return nextLeftTuple;
    }

    private Tuple getNextRightTuple() {
        if (rightBatch == null) {
            return null;
        } else if (rightPtr == rightBatch.size()) {
            rightBatch = rightSorted.next();
            rightPtr = 0;
        }

        if (rightBatch == null || rightBatch.size() <= rightPtr) {
            return null;
        }

        Tuple next = rightBatch.get(rightPtr);
        rightPtr++;
        return next;
    }

    private void createRightPartition() {
        rightPartition.clear();
        int result = 0;
        if (peekRightTuple == null) {
            peekRightTuple = getNextRightTuple();
            if (peekRightTuple == null) {
                return;
            }
        }

        while (result == 0) {
            rightPartition.add(peekRightTuple);
            peekRightTuple = getNextRightTuple();
            if (peekRightTuple == null) {
                break;
            }
            result = Tuple.compareTuples(rightPartition.get(0), peekRightTuple, rightIndices, rightIndices);
        }
    }

    public boolean close() {
        leftSorted.close();
        rightSorted.close();
        return true;
    }

}