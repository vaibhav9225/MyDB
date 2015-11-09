package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

// Class to remove unnecessary columns

public class Compressor implements Operator{
	private Operator oper;
	private ArrayList<Column> schema;
	private Integer[] ignores;

	public Compressor(Operator oper, String table) {
		this.oper = oper;
		ignores = Code.getSelectionMap(table);
		init();
	}

	private void init(){
		schema = new ArrayList<Column>();
		ArrayList<Column> tempSchema = oper.getSchema();
		for(int i=0; i<ignores.length; i++){
			schema.add(tempSchema.get(ignores[i]));
		}
	}
	
	@Override
	public ArrayList<LeafValue> readOneTuple() {
		ArrayList<LeafValue> tuple = oper.readOneTuple();
		ArrayList<LeafValue> newTuple = new ArrayList<LeafValue>();
		if(tuple == null) return null;
		else{
			for(int i=0; i<ignores.length; i++){
				newTuple.add(tuple.get(ignores[i])); // Remove columns that are not required.
			}
			return newTuple;
		}
	}

	@Override
	public ArrayList<Column> getSchema() {
		return schema;
	}

	@Override
	public void reset() {
		oper.reset();
		init();
	}

	@Override
	public void dump() {
		Output.dump(this);
	}
}