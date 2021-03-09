package qp.operators;

import java.util.ArrayList;

import jdk.javadoc.internal.doclets.formats.html.SourceToHTMLConverter;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Distinct is to be called as the root of the query tree.
 * It's base should be the entire query tree (usually a select or project operator)
 * Distinct will then sort it's input base, and do a single pass over the sorted tuples to remove
 * duplicates along the way.
 */
public class Distinct extends Operator {
	Operator base;					//underlying unsorted base
	Operator sortedbase;			//base will have to be sorted on all attrs to be able to remove duplicates in 1 pass
	Batch inputbuffer = null;
	Batch outputbuffer = null;
	
	boolean eos = false;	//true if have exhausted the underlying sorted base

	int inputindex = 0;
	int batchsize;
	Tuple lastseentuple = null;
	
	public Distinct(Operator base, int optype) {
		super(optype);
		this.base = base;
	}
	
	public Operator getBase() {
		return base;
	}
	
	public Operator getSortedBase() {
		return sortedbase;
	}

	/**
	 * what am i supposed to do in open()?
	 * get Distinct ready for others to call next() on it??
	 */
	@Override
    public boolean open() {
		int tupleSize = schema.getTupleSize();
		this.batchsize = Batch.getPageSize() / tupleSize;
		
		if (!base.open()) return false;
			
		/**
		 * distinct will do a single scan over the tuples of the file, rejecting 
		 * tuples if it is the same as the last tuple added. Hence have to sort the 
		 * underlying base.
		 * Base is sorted on ALL its attributes
		 */
		ArrayList<Attribute> sortattrs = base.getSchema().getAttList();
		ExternalSort extsort = new ExternalSort(base, sortattrs, BufferManager.numBuffer);
		sortedbase = extsort;

		//if the sorted based has error opening, cannot feed result pages to caller
		boolean canopen = sortedbase.open();
        return canopen;
    }

	/**
	 * each call to next must return the next Batch of 
	 * distinct tuples.
	 */
	@Override
    public Batch next() {
		if (eos) {
			//this operation is completely done
			//next() should never be called again
			this.close();
			return null;
		} 
		
		if (inputbuffer == null) {
			//first time calling this function, 
			//have to populate 
			inputbuffer = base.next();
		}
		
		//read in tuple by tuple and remove duplicates
		Batch outputbuffer = new Batch(this.batchsize);
		//keep adding distinct tuples to the outbuffer
		//write outbuffer once it's full or out of inputbuffers
		while (!outputbuffer.isFull()) {
			//add next tuple if possible, so
			//get next tuple if possible. else we've completed the scan
			//not possible when: we need a fresh buffer page || exhausted sorted base

			if (inputindex == inputbuffer.size()) {	//finished this inputbuffer, get next one
				inputbuffer = sortedbase.next();
				inputindex = 0;
			}
			if (inputbuffer == null) {	//sorted base exhausted, return whatever we have
				eos = true;
				return outputbuffer;
			} 
			
			//next tuple should exist now
			Tuple nexttuple = inputbuffer.get(inputindex);
			//nexttup belongs in output buffer if it's unique
			//nexttup in unique if it's the first tuple ever || it's diff from the last seen one
			if (lastseentuple == null) {
				outputbuffer.add(nexttuple);
			} else {
				//only add if it's different from lastseentup
				ArrayList<Integer> lastseenindex = new ArrayList<>();
				ArrayList<Integer> nexttupleindex = new ArrayList<>();
				if (lastseentuple.data().size() != nexttuple.data().size()) {
					System.out.println("ERROR! tuples should not have different sizes");
					System.exit(1);
				}
				for (int i = 0; i < lastseentuple.data().size(); ++i) {
					lastseenindex.add(i);
					nexttupleindex.add(i);
				}
				//tuple comparison will return zero if tuples are equal, non-zero if different
				if (Tuple.compareTuples(lastseentuple, nexttuple, lastseenindex, nexttupleindex) != 0) {
					outputbuffer.add(nexttuple);
				}
			}
			//update last seen
			lastseentuple = nexttuple;
			++inputindex;
		}
		return outputbuffer;
    }
	
	@Override
    public boolean close() {
		base.close();
		sortedbase.close();
        return true;
    }

	@Override
    public Object clone() {
        Distinct newD = new Distinct(base, this.optype);
        return newD;
    }
	
	
}
