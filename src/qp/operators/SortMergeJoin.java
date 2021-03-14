package qp.operators;

import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
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
    ArrayList<Tuple> rightPartition;  // collection of duplicates on right table
    int rightPartitionPtr;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        // sort left and right tables
        left.open();
        right.open();

        // batch size = num of tuples per batch
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftIndices = new ArrayList<>();
        rightIndices = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftIndices.add(left.getSchema().indexOf(leftattr));
            rightIndices.add(right.getSchema().indexOf(rightattr));
        }

        // initialize values
        leftPtr = 0;
        rightPtr = 0;
        eosl = false;
        eosr = false;
        leftTuple = null;
        rightTuple = null;
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
            leftBatch = left.next();
        }
        if (rightBatch == null) {
            rightBatch = left.next();
        }

        outBatch = new Batch(batchSize);

        while (true) {
            // if left batch is empty
            if (leftBatch == null) {
                eosl = true;
                return null;
            }
            // if right batch is empty
            if (rightBatch == null) {
                eosr = true;
                return null;
            }
            // if output batch is full, return it
            if (outBatch.isFull()) {
                return outBatch;
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
                    rightTuple = getNextRightTuple();
                }

                // once equal: put right tuple into right partition
                rightPartition.add(rightTuple);
                rightPartitionPtr = 0;
            }

            
            if (leftTuple.checkJoin(rightTuple, leftIndices, rightIndices)) {
                // put join result into outBatch
                Tuple joinResult = leftTuple.joinWith(rightTuple);
                outBatch.add(joinResult);   
                // advance right ptr and save into right partition            
                rightTuple = getNextRightTuple();
                rightPartition.add(rightTuple);
                rightPartitionPtr++;
            } else {
                // means right tuple has gone too far
                // advance left ptr and loop to the start of right partition     
                leftTuple = getNextLeftTuple();
                rightPartitionPtr = 0;
            }
        }

    }
    
    private Tuple getNextLeftTuple() {
        if (leftPtr == batchSize - 1) {
            leftBatch = left.next();
            if (leftBatch == null) {
                eosl = true;
                return null;
            }
        }

        leftPtr = (leftPtr + 1) % batchSize;
        return leftBatch.get(leftPtr);
    }

    private Tuple getNextRightTuple() {
        if (rightPtr == batchSize - 1) {
            rightBatch = right.next();
            if (rightBatch == null) {
                eosr = true;
                return null;
            }
        }

        rightPtr = (rightPtr + 1) % batchSize;
        return rightBatch.get(rightPtr);
    }
    

    public boolean close() {
        left.close();
        right.close();
        return true;
    }

}
