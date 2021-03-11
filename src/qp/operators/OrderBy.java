/**
 * OrderBy Operator
 **/

package qp.operators;

import qp.operators.OrderByType;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

// should we extend sort instead?
public class OrderBy extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch
    ExternalSort sortOp;           // Sort operator for base operator
    int opType;                    // Operator Type
    OrderByType orderType;            // OrderBy Type

    Batch inbatch;
    Batch outbatch;

    public OrderBy(Operator base, int opType, OrderByType type) {
        this.base = base;
        this.opType = opType;
        this.orderType = type;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        /**
		 * Base is sorted on ALL its attributes depending on the orderby type.
         * External sort should sort based on different comparator based on orderby type.
		 */
		ArrayList<Attribute> attributeList = base.getSchema().getAttList();
        sortOp = new ExternalSort(base, attributeList, numBuffer, orderType);
        return sortOp.open();
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        sortOp.close();
        return true;
    }

}
