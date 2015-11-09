package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

/*

A interface that is implemented by all SQL operators which dictates the basic operations it must perform.

*/

public interface Operator {
	
	public ArrayList<LeafValue> readOneTuple(); // Read one row at a time.
	public ArrayList<Column> getSchema(); // Get the current schema after "N" operations.
	public void reset(); // Reset the iteration pointer.
	public void dump(); // Print all the rows.
}