/** 
 * Performs external sort on the file
 */
package qp.operators;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class ExternalSort extends Operator {
    Operator base;      // base table to sort
    Schema schema;      // base table schema
    int batchSize;      // number of tuples per batch
    int numBuffer;      // number of buffer available
    ArrayList<Integer> attributeIndices = new ArrayList<>();    // index of attributes to sort on

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
        
        // number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // generate sorted runs based on batch size
        int numSortedRun = createSortedRuns();

        // merge sorted runs 
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
                // same as filling up one buffer
                tuplesInSortedRun.addAll(inputBatch.getTuples());

                if (base.next() != null) {
                    inputBatch = base.next();
                }
            }
            numSortedRun++;

            // sort tuples
            tuplesInSortedRun.sort(this::tupleComparator);
            
            // generating of sorted runs = phase 0 
            writeTuplesToFile(tuplesInSortedRun, numSortedRun, 0);
                
            inputBatch = base.next();
        }
        return numSortedRun;
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
    
    public void writeTuplesToFile(ArrayList<Tuple> sortedTuples, int sortedRunNum, int passNum) {
        try {
            // add to file
            FileOutputStream fileOut = new FileOutputStream("sorted_run_" + sortedRunNum + "_pass_" + passNum);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            for (Tuple tuple : sortedTuples) {
                objectOut.writeObject(tuple);
            }
            objectOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void mergeSortedRuns(int numSortedRun) {
        int numBufferAvail = numBuffer = 1;
        int numRunsUnsorted = numSortedRun;
        int passId = 0;
        
        while (numRunsUnsorted > 1) {
            kWayMerge(numRunsUnsorted, passId, numBufferAvail);
            double div = numRunsUnsorted / numBufferAvail;
            numRunsUnsorted = (int) Math.ceil(div);
            passId++;
        }
    }

    public void kWayMerge(int numRunsUnsorted, int passId, int numBufferAvail) {
        
    }

    /**
     * Sorts next tuple from operator
     */

    public Batch next() {
        return new Batch(batchSize);
    }

    /**
     * Close the operator
     */
    public boolean close() {
        super.close();
        
        return true;
    }

}
