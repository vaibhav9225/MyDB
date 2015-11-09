package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

/*

Class that performs the UNION operation
Takes two or more operators as input and merges the output from all of them

*/

public class Combine implements Operator{
	ArrayList<Operator> opers;
	int index;

	public Combine(ArrayList<Operator> opers){
		this.opers = opers;
		index = 0;
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() {
		if(index < opers.size()){
			ArrayList<LeafValue> tuple = opers.get(index).readOneTuple();
			if(tuple == null){
				index++;
				return this.readOneTuple();
			}
			return tuple;
		}
		return null;
	}

	@Override
	public ArrayList<Column> getSchema() {
		return opers.get(0).getSchema();
	}

	@Override
	public void reset() {
		for(Operator oper : opers){
			oper.reset();
		}
		index = 0;
	}

	@Override
	public void dump() {
		Output.dump(this);
	}
}