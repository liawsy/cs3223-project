package qp.operators;

import java.nio.Buffer;
import java.util.ArrayList;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * As per Prof Tan, groupby just makes the output tuples grouped together without removing duplicates.
 * A table sorted on the groupby attributes is necessarily grouped by those attributes. 
 * Hence groupby will just sort its base on the groupby attrlist.
 * 
 * Internally uses an external sort
 */
public class GroupBy extends Operator {
	Operator base;					//underlying unsorted base
	ArrayList<Attribute> groupbylist;

	Operator groupedbase;			//base will have to be sorted on grouped attrs
	int batchsize;
	
	public GroupBy(Operator base, ArrayList<Attribute> groupbylist, int optype) {
		super(optype);
		this.base = base;
		this.groupbylist = groupbylist;
	}

	/**
	 * what am i supposed to do in open()?
	 * get Distinct ready for others to call next() on it??
	 */
	@Override
    public boolean open() {
		System.out.println("groupby open()");
		int tupleSize = schema.getTupleSize();
		this.batchsize = Batch.getPageSize() / tupleSize;
		
		if (!base.open()) return false;
		System.out.println("groupby after ret false");
			
		/**
		 * Create the underlying sort operator on the groupby list
		 */
		ExternalSort extsort = new ExternalSort(base, groupbylist, BufferManager.getNumBuffer());
		//ExternalSortStub extsort = new ExternalSortStub(base, groupbylist, BufferManager.getNumBuffer());
		groupedbase = extsort;

		//if the sorted based has error opening, cannot feed result pages to caller
		System.out.println("before groupedbase open");
		boolean canopen = groupedbase.open();
		System.out.println("after groupedbase open");
        return canopen;
    }


	@Override
    public Batch next() {
		return groupedbase.next();
    }

	@Override
    public Object clone() {
		//must deep clone EVERYTHING
		Operator newbase = (Operator) this.base.clone();
		//Schema newschema = (Schema) this.schema.clone();
		Schema newschema = (Schema) newbase.getSchema();

		ArrayList<Attribute> newgroupbylist = new ArrayList<>();
		for (Attribute a : this.groupbylist) {
			newgroupbylist.add((Attribute) a.clone());
		}

        GroupBy newgb = new GroupBy(newbase, newgroupbylist, this.optype);
		newgb.setSchema(newschema);
        return newgb;
    }
		
	public void setBase(Operator base) {
		this.base = base;
	}

	public Operator getBase() {
		return base;
	}
	
	public Operator getGroupedBase() {
		return groupedbase;
	}
}
