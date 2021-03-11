package qp.operators;

import qp.utils.Batch;

public class SortMergeJoin extends Join {
    int batchsize;                  // Number of tuples per out batch

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        return true;
    }

    public Batch next() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        return new Batch(batchsize);
    }

    public boolean close() {
        return true;
    }

}
