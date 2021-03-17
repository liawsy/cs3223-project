/**
 * OrderBy Operator
 **/

package qp.operators;

import qp.operators.ExternalSort;

import qp.optimizer.BufferManager;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OrderBy extends Operator {

    Operator base;                 // Base table to project
    ExternalSort sortOp;           // Sort operator for base operator

    ArrayList<Attribute> attributeList;   // List of Attributes for Orderby

    int opType;                    // Operator Type
    int order;                     // Order in which tuples should appear
    int numBuffer;                 // Number of buffers
    int batchSize;                 // Number of tuples per outbatch

    public OrderBy(Operator base, int opType, int order, ArrayList<Attribute> attributeList) {
        super(opType);
        this.base = base;
        this.opType = opType;
        this.order = order;
        this.attributeList = attributeList;
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

    @Override
    public boolean open() {
		int tupleSize = this.schema.getTupleSize();
		this.batchSize = Batch.getPageSize() / tupleSize;

		if (!base.open()) return false;

		/**
		 * Create the underlying sort operator on the orderby list
		 */
        if (order == OrderByType.ASC) {
            // sortOp = new ExternalSort(base, attributeList, BufferManager.getNumBuffer(), false);
        } else {
            // sortOp = new ExternalSort(base, attributeList, BufferManager.getNumBuffer(), true);
        }
        
        return sortOp.open();
    }


    @Override
    public Batch next() {
		return sortOp.next();
    }

	@Override
    public Object clone() {
		//must deep clone EVERYTHING
		Operator newBase = (Operator) this.base.clone();
		Schema newSchema = (Schema) newBase.getSchema();

		ArrayList<Attribute> newOrderByList = new ArrayList<>();
		for (Attribute a : this.attributeList) {
			newOrderByList.add((Attribute) a.clone());
		}

        OrderBy newOb = new OrderBy(newBase, this.opType, order, newOrderByList);
		newOb.setSchema(newSchema);
        return newOb;
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
