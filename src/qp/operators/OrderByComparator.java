
package qp.operators;

import qp.utils.*;

import qp.operators.OrderByType;

public class OrderByComparator  implements Comparator<Tuple> {
    
    OrderByType obType;

    public OrderByComparator(OrderByType obType) {
        this.obType = obType;
    }

    public int compare(Tuple t1, Tuple t2) {
        return (obType == ASC) ? getAscOrder(t1, t2) : getDescOrder(t1, t2);
    }

    /**
     * 
     * @param t1 is the first tuple
     * @param t2 is the second tuple
     * @return -1 if t1 comes after t2, 0 if they are equal, 1 if t1 comes before t2
     */
    public int getDescOrder(Tuple t1, Tuple t2) {
        int result = 0;
        for (int i = 0; i < attributeIndices.size(); i++) {
            result = Tuple.compareTuples(t1, t2, attributeIndices.get(i)) * -1;
            if (result != 0) {
                return result;
            }
        }
        return result;
    }

    /**
     * 
     * @param t1 is the first tuple
     * @param t2 is the second tuple
     * @return -1 if t1 comes before t2, 0 if they are equal, 1 if t2 comes before t1
     */
    public int getAscOrder(Tuple t1, Tuple t2) {
        int result = 0;
        for (int i = 0; i < attributeIndices.size(); i++) {
            result = Tuple.compareTuples(t1, t2, attributeIndices.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }

}
