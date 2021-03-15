/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators;

public class JoinType {

    public static final int NESTEDJOIN = 0;
    // change the ordering later
    public static final int SORTMERGE = 1;
    public static final int BLOCKNESTED = 2;
    public static final int HASHJOIN = 3;

    public static int numJoinTypes() {
        return 2;
    }
}
