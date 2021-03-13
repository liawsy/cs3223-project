/**
 * OrderBy Operator
 **/

package qp.operators;

import qp.operators.ExternalSort;

import qp.optimizer.BufferManager;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.OrderByComparator;
import qp.utils.OrderByType;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

// should we extend sort instead?
public class OrderBy extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch
    ExternalSort sortOp;           // Sort operator for base operator
    int opType;                    // Operator Type
    // List<OrderByType> orderTypes;  // List of Orderby Types
    int order;              // order in which tuples should appear
    ArrayList<Attribute> attributeList;   // List of Attributes for Orderby
    ArrayList<Integer> attributeIndices = new ArrayList<>(); // List of Attribute Indices
    int numBuffer;                 // Number of buffers
    OrderByComparator tupleComparator; // Comparator for External Sort

    public OrderBy(Operator base, int opType, int order, ArrayList<Attribute> attributeList) {
        super(opType);
        this.base = base;
        this.opType = opType;
        this.order = order;
        this.attributeList = attributeList;
    }

    public OrderBy(Operator base, int opType, int order, ArrayList<Attribute> attributeList, int numBuffer) {
        super(opType);
        this.base = base;
        this.opType = opType;
        this.order = order;
        this.attributeList = attributeList;
        this.numBuffer = numBuffer;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getNumBuff() {
        return this.numBuffer;
    }

    public void setNumBuff(int num) {
        this.numBuffer = num;
    }

    private void updateAttributeIndices() {

        for (int i = 0; i < attributeList.size(); i++) {
            Attribute attribute = attributeList.get(i);
            attributeIndices.add(this.schema.indexOf(attribute));
        }
    }

    @Override
    public boolean open() {
		int tupleSize = this.schema.getTupleSize();
		this.batchsize = Batch.getPageSize() / tupleSize;

		if (!base.open()) return false;

        updateAttributeIndices();

        tupleComparator = new OrderByComparator(attributeIndices, order);

		/**
		 * Create the underlying sort operator on the groupby list
		 */
        sortOp = new ExternalSort(base, attributeList, BufferManager.getNumBuffer(), tupleComparator);
        return sortOp.open();
    }


    @Override
    public Batch next() {
		return sortOp.next();
    }

	@Override
    public Object clone() {
		//must deep clone EVERYTHING
		Operator newbase = (Operator) this.base.clone();
		Schema newschema = (Schema) newbase.getSchema();

		ArrayList<Attribute> newOrderByList = new ArrayList<>();
		for (Attribute a : this.attributeList) {
			newOrderByList.add((Attribute) a.clone());
		}

        OrderBy newob = new OrderBy(newbase, this.opType, order, newOrderByList);
		newob.setSchema(newschema);
        return newob;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        base.close();
        sortOp.close();
        return true;
    }

}
