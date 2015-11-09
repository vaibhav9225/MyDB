package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

// Class that LIMITS the number of tuples that need to be printed. Emulates LIMIT operator.

public class Limit implements Operator{
	private Operator oper;
	private long limit;
	private long count;

	public Limit(Operator oper, long limit) {
		this.oper = oper;
		this.limit = limit;
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() {
		if(count < limit){
			count++;
			return oper.readOneTuple();
		}
		return null;
	}

	@Override
	public ArrayList<Column> getSchema() {
		return oper.getSchema();
	}

	@Override
	public void reset() {
		oper.reset();
	}

	@Override
	public void dump() {
		Output.dump(this);
	}
}
