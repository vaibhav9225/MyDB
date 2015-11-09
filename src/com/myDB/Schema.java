package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

// A simple class that is used to restructure the schema of the operator.

public class Schema implements Operator{
	private Operator oper;
	private String alias;
	private ArrayList<Column> schema = new ArrayList<Column>();

	public Schema(Operator oper, String alias) {
		this.oper = oper;
		this.alias = alias;
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() {
		return oper.readOneTuple();
	}

	@Override
	public ArrayList<Column> getSchema() {
		ArrayList<Column> schemaTemp = oper.getSchema();
		for(Column col : schemaTemp){
			Table table = new Table();
			table.setName(alias); // Use alias as new column name.
			Column column = new Column(table, col.getColumnName());
			schema.add(column);
		}
		return schema;
	}

	@Override
	public void reset() {
		oper.reset();
	}

	@Override
	public void dump() {
		oper.dump();
	}
}
