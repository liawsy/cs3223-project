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
        buffPerJoin = numBuffer / numJoin;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
