/** 
 * Performs external sort on the file
 */
package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class Sort extends Operator {
    Operator base;      // base table to sort
    Schema schema;      // base table schema
    int batchsize;      // number of tuples per batch
    int numBuffer;      // number of buffer available

    public Sort(Operator operator, int numBuffer) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numBuffer = numBuffer;
    }

    /**
     * opens connection to the base operator
     */
    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }
        System.out.println("HELLOHELLOHELLOHELLOHELLOHELLO");
        // number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tupleSize;

        // generate sorted runs based on number of buffers
        CreateSortedRuns(numBuffer);

        return true;
    }

    /**
     * Sorts next tuple from operator
     */

    public Batch next() {
        return new Batch(batchsize);
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return true;
    }

    public void CreateSortedRuns(int numBuffer) {
        // with B buffer pages, read in B pages each time
        // sort records and produce a B page sorted run
        // num_sorted_runs is ceiling (N/B)
    }
    
    public void MergeSortedRuns() {

    }

}
