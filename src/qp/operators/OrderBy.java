/**
 * OrderBy Operation
 **/

package qp.operators;

import qp.utils.*;

public class OrderBy extends Operator {

    Operator base;  // Base operator
    Condition con;  // Select condition
    int batchsize;  // Number of tuples per outbatch

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    ExternalSort sortOp; // Sort operator to be applied on base operator

    /**
     * constructor
     **/
    public OrderBy(Operator base, Condition con, int type) {
        super(type);
        this.base = base;
        this.con = con;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }


}