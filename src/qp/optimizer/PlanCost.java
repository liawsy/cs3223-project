/**
 * This method calculates the cost of the generated plans
 * also estimates the statistics of the result relation
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class PlanCost {

    long cost;
    long numtuple;

    /**
     * If buffers are not enough for a selected join
     * * then this plan is not feasible and return
     * * a cost of infinity
     **/
    boolean isFeasible; //is set to true when calculateCost() doesn't support the Operator subtype passed in

    /**
     * Hashtable stores mapping from Attribute name to
     * * number of distinct values of that attribute
     **/
    HashMap<Attribute, Long> ht;


    public PlanCost() {
        ht = new HashMap<>();
        cost = 0;
    }

    /**
     * Returns the cost of the plan
     **/
    public long getCost(Operator root) {
        cost = 0;
        isFeasible = true;
        numtuple = calculateCost(root);
        if (isFeasible) {
            return cost;
        } else {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }
    }

    /**
     * Get number of tuples in estimated results
     **/
    public long getNumTuples() {
        return numtuple;
    }


    /**
     * Returns number of tuples in the root
     **/
    protected long calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return getStatistics((Distinct) node);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return getStatistics((GroupBy) node);
        } else if (node.getOpType() == OpType.ORDERBY) {
            return getStatistics((OrderBy) node);
        }  
        System.out.println("operator is not supported");
        isFeasible = false;
        return 0;
    }

    /**
     * groupby doesn't change the number of output tuples, only reorders them.     * 
     * groupby's IO cost is the cost to sort the underlying base
     * @param node
     * @return
     */
    protected long getStatistics(GroupBy node) {
        /**
         * IO cost: need 
         * - #pages of base, |N|
         * - #buffers, B
         * - log function, double Math.log(double)
         * - ceil function, double Math.ceil(double)
         */
        
         //calculating number of output tuples
         long numouttuples = calculateCost(node.getBase());
 
         //incrementing IO cost
         int pagecapacity = Batch.getPageSize() / node.getSchema().getTupleSize();//implicit floor bc of integer division
         int numpages = (int) Math.ceil((double)numouttuples / (double)pagecapacity);
         //following the generate cost for sort merge
         long numpasses = 1 + (long)Math.ceil(
                                        Math.log(Math.ceil((double)numpages / (double)BufferManager.numBuffer)) / 
                                        Math.log(BufferManager.numBuffer - 1));
         long numIO = 2 * numpages * numpasses;
         cost += numIO;
 
         return numouttuples;
         //won't change the number of distinct values in each attr, so no update to ht
     }

    /**
     * Distinct might reduce the number of output tuples, 
     * but does not modify the schema of the base table, hence 
     * number of tuples per page should be unchanged.
     * Incurs IO cost equal to doing 1 ExternalSort internally
     */
    protected long getStatistics(Distinct node) {
        /**
         * num tuples output is MIN of
         * - #tuples in input
         * - #possible distinct tuples
         * 
         * #possible distinct tuples is product of all distinct values of all cols of the tuple's schema
         * but #possible can overflow easily, return early if #possible >= #outputtuples
         * 
         * IO cost: need 
         * - #pages of base, |N|
         * - #buffers, B
         * - log function, double Math.log(double)
         * - ceil function, double Math.ceil(double)
         */
        
         //calculating number of output tuples
        long numouttuples = calculateCost(node.getBase());  //numouttuples of base and sortedbase should be the same
        long numpossibletuples = 1;//might overflow bc multiplying multiple longs tgt use for loop to exit before that
        for (int i = 0; i < node.getSchema().getAttList().size(); ++i) {
            Attribute attrholder = node.getSchema().getAttList().get(i);
            numpossibletuples *= ht.get(attrholder);
            if (numpossibletuples >= numouttuples) {
                break;
            }
        }
        if (numouttuples <= 0 || numpossibletuples <= 0) { System.out.println("Suspect long overflow"); System.exit(1);}
        
        numouttuples = Math.min(numouttuples, numpossibletuples);

        //incrementing IO cost
        int pagecapacity = Batch.getPageSize() / node.getSchema().getTupleSize();//implicit floor bc of integer division
        int numpages = (int) Math.ceil((double)numouttuples / (double)pagecapacity);
        //following the generate cost for sort merge
        long numpasses = 1 + (long)Math.ceil(
                                       Math.log(Math.ceil((double)numpages / (double)BufferManager.numBuffer)) / 
                                       Math.log(BufferManager.numBuffer - 1));
        long numIO = 2 * numpages * numpasses;
        cost += numIO;

        return numouttuples;
        //i don't think distinct will change the number of distinct values in each attr, so no update to ht
    }

    /**
     * Projection will not change any statistics
     * * No cost involved as done on the fly
     **/
    protected long getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Calculates the statistics and cost of join operation
     **/
    protected long getStatistics(Join node) {
        long lefttuples = calculateCost(node.getLeft());
        long righttuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return 0;
        }

        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /** Get size of the tuple in output & correspondigly calculate
         ** buffer capacity, i.e., number of tuples per page **/
        long tuplesize = node.getSchema().getTupleSize();
        long outcapacity = Math.max(1, Batch.getPageSize() / tuplesize);
        long leftuplesize = leftschema.getTupleSize();
        long leftcapacity = Math.max(1, Batch.getPageSize() / leftuplesize);
        long righttuplesize = rightschema.getTupleSize();
        long rightcapacity = Math.max(1, Batch.getPageSize() / righttuplesize);
        long leftpages = (long) Math.ceil(((double) lefttuples) / (double) leftcapacity);
        long rightpages = (long) Math.ceil(((double) righttuples) / (double) rightcapacity);

        double tuples = (double) lefttuples * righttuples;
        for (Condition con : node.getConditionList()) {
            Attribute leftjoinAttr = con.getLhs();
            Attribute rightjoinAttr = (Attribute) con.getRhs();
            int leftattrind = leftschema.indexOf(leftjoinAttr);
            int rightattrind = rightschema.indexOf(rightjoinAttr);
            leftjoinAttr = leftschema.getAttribute(leftattrind);
            rightjoinAttr = rightschema.getAttribute(rightattrind);

            /** Number of distinct values of left and right join attribute **/
            long leftattrdistn = ht.get(leftjoinAttr);
            long rightattrdistn = ht.get(rightjoinAttr);
            tuples /= (double) Math.max(leftattrdistn, rightattrdistn);
            long mindistinct = Math.min(leftattrdistn, rightattrdistn);
            ht.put(leftjoinAttr, mindistinct);
            ht.put(rightjoinAttr, mindistinct);
        }
        long outtuples = (long) Math.ceil(tuples);

        /** Calculate the cost of the operation **/
        int joinType = node.getJoinType();
        long numbuff = BufferManager.getBuffersPerJoin();
        long joincost;

        switch (joinType) {
            case JoinType.NESTEDJOIN:
                joincost = leftpages * rightpages;
                break;
            case JoinType.SORTMERGE:
                // cost to sort left
                long sortLeftCost = externalSortCost(leftpages, numbuff);
                // cost to sort right
                long sortRightCost = externalSortCost(rightpages, numbuff);
                // merge cost
                long mergeCost = leftpages + rightpages;
                joincost = sortLeftCost + sortRightCost + mergeCost;
                break;
            case JoinType.BLOCKNESTED:
                long outerblocks = numbuff - 2;
                joincost = ((int) Math.ceil(leftpages/outerblocks)) * rightpages;
                break;
            default:
                System.out.println("join type is not supported");
                return 0;
        }
        cost = cost + joincost;

        return outtuples;
    }

    private long externalSortCost(long numPages, long numBuff) {
        int numPass = 1 + (int) Math.ceil(Math.log(Math.ceil(numPages / (1.0 * numBuff))) / Math.log(numBuff - 1) );
        return numPass * (2 * numPages);
    }
    protected long getStatistics(OrderBy node) {
        /**
         * IO cost: need 
         * - #pages of base, |N|
         * - #buffers, B
         * - log function, double Math.log(double)
         * - ceil function, double Math.ceil(double)
         */
        
         //calculating number of output tuples
         long numouttuples = calculateCost(node.getBase());
 
         //incrementing IO cost
         int pagecapacity = Batch.getPageSize() / node.getSchema().getTupleSize();//implicit floor bc of integer division
         int numpages = (int) Math.ceil((double)numouttuples / (double)pagecapacity);
         //following the generate cost for sort merge
         long numpasses = 1 + (long)Math.ceil(
                                        Math.log(Math.ceil((double)numpages / (double)BufferManager.numBuffer)) / 
                                        Math.log(BufferManager.numBuffer - 1));
         long numIO = 2 * numpages * numpasses;
         cost += numIO;
 
         return numouttuples;
    }
    

    /**
     * Find number of incoming tuples, Using the selectivity find # of output tuples
     * * And statistics about the attributes
     * * Selection is performed on the fly, so no cost involved
     **/
    protected long getStatistics(Select node) {
        long intuples = calculateCost(node.getBase());
        if (!isFeasible) {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);
        int exprtype = con.getExprType();

        /** Get number of distinct values of selection attributes **/
        long numdistinct = intuples;
        Long temp = ht.get(fullattr);
        numdistinct = temp.longValue();

        long outtuples;
        /** Calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (long) Math.ceil((double) intuples / (double) numdistinct); 
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (long) Math.ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            outtuples = (long) Math.ceil(0.5 * intuples);
        }

        //updating selected-attribute
        Condition cn = node.getCondition();
        if (cn.getOpType() != Condition.SELECT) {
            System.out.println("Error, expecting SELECT node");
            System.exit(1);
        }
        Attribute selectattr = cn.getLhs();
        long oldvalue = ht.get(selectattr);
        long newvalue = (long) Math.ceil(((double) outtuples / (double) intuples) * oldvalue);
        ht.put(selectattr, newvalue);  

        //updating non-selected attributes
        for (int i = 0; i < schema.getNumCols(); ++i) {
            Attribute attri = schema.getAttribute(i);
            if (!attri.equals(selectattr)) {    //only make changes for non-selected attributes
                oldvalue = ht.get(attri);
                newvalue = Math.min(oldvalue, outtuples);
                ht.put(attri, newvalue);
            }
        }

        return outtuples;
    }

    /**
     * The statistics file <tablename>.stat to find the statistics
     * * about that table;
     * * This table contains number of tuples in the table
     * * number of distinct values of each attribute
     **/
    protected long getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        long numtuples = Long.parseLong(temp);
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        for (int i = 0; i < numAttr; ++i) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Long distinctValues = Long.valueOf(temp);
            ht.put(attr, distinctValues);
        }

        /** Number of tuples per page**/
        long tuplesize = schema.getTupleSize();
        long pagesize = Math.max(Batch.getPageSize() / tuplesize, 1);
        long numpages = (long) Math.ceil((double) numtuples / (double) pagesize);

        cost = cost + numpages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }
        return numtuples;
    }

}











