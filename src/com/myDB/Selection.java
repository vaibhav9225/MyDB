package com.myDB;

import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.expression.*;

// Class to implement WHERE clause.

public class Selection implements Operator{
	private Operator oper;
	private Expression condition;
	private ConditionVisitor eval;
	
	public Selection (Operator oper, Expression condition, ArrayList<SelectExpressionItem> alias){
		this.oper = oper;
		this.condition = condition;
		eval = new ConditionVisitor(getSchema(), alias);
	}
	
	@Override
	public ArrayList<LeafValue> readOneTuple(){
		ArrayList<LeafValue> tuple = null;
		do{
			tuple = oper.readOneTuple();
			if (tuple == null){
				return null;
			}
			eval.setTuple(tuple);
			try { // Evaluate the where expression. If true, return tuple. Else, don't.
				if(!((BooleanValue) eval.eval(condition)).getValue()){
					tuple = null;
				}
			} catch (SQLException e) {}
		}
		while (tuple == null);
		return tuple;
	}
	
	@Override
	public void reset(){
		oper.reset();
	}

	@Override
	public void dump() {
		Output.dump(this);
	}

	@Override
	public ArrayList<Column> getSchema() {
		return oper.getSchema();
	}
}