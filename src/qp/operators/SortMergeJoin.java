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
        // terminate if left file reaches end (no more tuples to merge)
        if (eosl) {
            close();
            return null;
        }
        
        // 1. if no batch read yet, read one batch in from both sides
        if (leftBatch == null) {
            leftPtr = 0;
            leftBatch = leftSorted.next();
        }
        if (rightBatch == null) {
            rightPtr = 0;
            rightBatch = rightSorted.next();
        }

        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
            if (leftBatch == null) {
                eosl = true;
                return null;
            }
            if (rightBatch == null) {
                eosr = true;
                return null;
            }

            leftTuple = leftBatch.get(leftPtr);
            rightTuple = rightBatch.get(rightPtr);

            if (rightPartition.isEmpty()) {
                // while left < right: advance left
                while (Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices) < 0) {
                    leftTuple = getNextLeftTuple();
                }

                // while left > right: advance right
                while (Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices) > 0) {
                    rightTuple = peekNextRightTuple(rightPtr);
                    rightPtr = (rightPtr + 1) % batchSize;
                }

                // once equal: put right tuple into right partition
                rightPartition.add(rightTuple);
                rightPartitionPtr = 0;
            }

            Tuple rightPartitionTuple = rightPartition.get(rightPartitionPtr);
            
            if (leftTuple.checkJoin(rightPartitionTuple, leftIndices, rightIndices)) {
                // put join result into outBatch
                Tuple joinResult = leftTuple.joinWith(rightPartitionTuple);
                outBatch.add(joinResult);

                // check if there is a next in partition
                Tuple nextInRightPartition = null;
                if (rightPartitionPtr + 1 < rightPartition.size()) {
                    nextInRightPartition = rightPartition.get(rightPartitionPtr + 1);
                }
                
                if (nextInRightPartition == null) { 
                    // peek right tuple
                    if (peekRightTuple == null) {
                        peekRightTuple = peekNextRightTuple(rightPtr);
                        // reached the end of right table
                        if (peekRightTuple == null) {
                            eosr = true;
                            leftTuple = getNextLeftTuple();
                            // reached the end of left table
                            if (leftTuple == null) {
                                eosl = true;
                                leftBatch = null;
                                return outBatch;
                            }
                            rightPartitionPtr = 0;
                            rightPartitionTuple = rightPartition.get(rightPartitionPtr);
                            continue;
                        }
                    }       
                    
                    // either there are more right tuples to add OR reached the end of right partition
                    if (Tuple.compareTuples(rightPartitionTuple, peekRightTuple, rightIndices, rightIndices) == 0) {
                        rightPartition.add(peekRightTuple);
                        rightPartitionPtr++;
                        rightTuple = peekRightTuple;
                        rightPtr = (rightPtr + 1) % batchSize;
                        peekRightTuple = null;

                    } else if ((Tuple.compareTuples(rightPartitionTuple, peekRightTuple, rightIndices, rightIndices) > 0)) {
                        leftTuple = getNextLeftTuple();   
                        rightPartitionPtr = 0;
                    }
                } else {
                    rightPartitionPtr++;
                }

            } else {
                rightTuple = peekRightTuple;
                peekRightTuple = null;
                rightPartition.clear();
                rightPartitionPtr = 0;
            }
        }
        return outBatch;
    }
    
    private Tuple getNextLeftTuple() {
        if (leftPtr == leftBatch.size() - 1) {
            leftBatch = leftSorted.next();
            if (leftBatch == null) {
                eosl = true;
                return null;
            }
        }
        leftPtr = (leftPtr + 1) % batchSize;
        return leftBatch.get(leftPtr);
    }

    private Tuple peekNextRightTuple(int currIndex) {
        if (currIndex == rightBatch.size() - 1) {
            rightBatch = rightSorted.next();
            if (rightBatch == null) {
                // eosr = true;
                return null;
            }
        }
        int nextIndex = (currIndex + 1) % batchSize;
        return rightBatch.get(nextIndex);
    }

    public boolean close() {
        leftSorted.close();
        rightSorted.close();
        return true;
    }

}