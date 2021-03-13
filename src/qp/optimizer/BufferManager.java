/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        //BUG: ?don't we do 1 join at a time? why have to split the joins between all buffers?
        //ripple bug: if there are no joins, numjoin == 0 and we get division by zero.
        //this line is still run without joins bc now distinct uses sort which needs buffers
        //ORIGINAL LINE: buffPerJoin = numBuffer / numJoin;
        if (numJoin == 0) {
            buffPerJoin = numBuffer;
        } else {
            buffPerJoin = numBuffer / numJoin;
        }
    }

    public static int getNumBuffer() {
        return numBuffer;
    }
    
    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
