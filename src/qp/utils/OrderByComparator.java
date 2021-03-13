package qp.utils;

import qp.utils.Attribute;
import qp.utils.OrderByType;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Comparator;
import java.util.List;

public class OrderByComparator implements Comparator<Tuple> {
    
    List<Integer> attributeIndices;
    // List<OrderByType> orderByList;
    int order;
    Schema schema;


    // public OrderByComparator(List<Attrbute> attributeList, List<OrderBy> orderByList) {
    //     this.orderByList = orderByList;
    //     for (int i = 0; i < attributeList.size(); i++) {
    //         Attribute attribute = attributeList.get(i);
    //         attributeIndices.add(schema.indexOf(attribute));
    //     }
    // }

    public OrderByComparator(List<Integer> attributeIndices, int order) {
        this.order = order;
        this.attributeIndices = attributeIndices;
    }

    /**
     * 
     * @param t1 is the first tuple
     * @param t2 is the second tuple
     * @return -1 if t1 comes before t2, 0 if they are equal, 1 if t2 comes before t1
     */
    @Override
    public int compare(Tuple t1, Tuple t2) {
        int result = 0;
        for (int i = 0; i < attributeIndices.size(); i++) {
            result = Tuple.compareTuples(t1, t2, attributeIndices.get(i));
            if (result != 0) {
               if (order == OrderByType.ASC) {
                   return result;
               } else {
                   return result * -1;
               }
            }
        }
        return result;
    }

    // /**
    //  * 
    //  * @param t1 is the first tuple
    //  * @param t2 is the second tuple
    //  * @return -1 if t1 comes after t2, 0 if they are equal, 1 if t1 comes before t2
    //  */
    // public int getDescOrder(Tuple t1, Tuple t2) {
    //     int result = 0;
    //     for (int i = 0; i < attributeIndices.size(); i++) {
    //         result = Tuple.compareTuples(t1, t2, attributeIndices.get(i)) * -1;
    //         if (result != 0) {
    //             return result;
    //         }
    //     }
    //     return result;
    // }

    // /**
    //  * 
    //  * @param t1 is the first tuple
    //  * @param t2 is the second tuple
    //  * @return -1 if t1 comes before t2, 0 if they are equal, 1 if t2 comes before t1
    //  */
    // public int getAscOrder(Tuple t1, Tuple t2) {
    //     int result = 0;
    //     for (int i = 0; i < attributeIndices.size(); i++) {
    //         result = Tuple.compareTuples(t1, t2, attributeIndices.get(i));
    //         if (result != 0) {
    //             return result;
    //         }
    //     }
    //     return result;
    // }

}
