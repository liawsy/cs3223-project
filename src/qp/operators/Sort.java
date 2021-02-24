
package qp.operators;

public class Sort extends Operator {
    public Sort(Operator operator) {
        super(OpType.SORT);
    }

    public void CreateSortedRuns() {
        // with B buffer pages, read in B pages each time
        // sort recrods and produce a B page sorted run
        // num_sorted_runs is ceiling (N/B)
    }
    
    public void MergeSortedRuns() {

    }

}
