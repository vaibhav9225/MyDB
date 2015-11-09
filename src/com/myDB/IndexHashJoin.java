package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

/* 

Class that Implements Indexed Hash Join

*/

public class IndexHashJoin implements Operator {
	private Operator oper;
	private IndexedRelation operJoin;
	private ArrayList<Column> schema;
	private ArrayList<ArrayList<LeafValue>> batch;
	private int pointer = 0;
	private int buffer = -1;
	private int leftIndex = 0;
	private ArrayList<LeafValue> tuple = null;
	private ArrayList<Expression> wList = new ArrayList<Expression>();
	
	
	// Read two tables, one indexed and other non indexed operator and intialize the schema.
	public IndexHashJoin(Operator oper, IndexedRelation operJoin, Expression condition){
		this.oper = oper;
		this.operJoin = operJoin;
		whereExpr(condition);
		for(Expression expr : wList){
			String left = ((BinaryExpression) expr).getLeftExpression().toString();
			String right = ((BinaryExpression) expr).getRightExpression().toString();
			ArrayList<Column> schema = oper.getSchema();
			for(int i=0; i<schema.size(); i++){
				String strT = schema.get(i).getTable().getName() + "." + schema.get(i).getColumnName();
				String strA = schema.get(i).getTable().getAlias() + "." + schema.get(i).getColumnName();
				if(left.equals(strT) || left.equals(strA)){
					leftIndex = i;
					break;
				}
				else if(right.equals(strT) || right.equals(strA)){
					leftIndex = i;
					break;
				}
			}
		}
		reset();
	}
	
	public void whereExpr(Expression expr){
		if(expr instanceof AndExpression){
			AndExpression exp = (AndExpression) expr;
			whereExpr(exp.getLeftExpression());
			whereExpr(exp.getRightExpression());
		}
		else{
			wList.add(expr);
		}
	}
	
	@Override
	public ArrayList<LeafValue> readOneTuple() {
		if(buffer == -1 || pointer == buffer){
			while(true){
				tuple = oper.readOneTuple();
				if(tuple == null) return null;
				String key = "";
				pointer = 0;
				buffer = -1;
				key = tuple.get(leftIndex).toString();
				batch = operJoin.getBatch(key); // Try to find the key in the index and fetch results.
				if(batch != null){
					buffer = batch.size();
					ArrayList<LeafValue> newTuple = new ArrayList<LeafValue>(tuple);
					newTuple.addAll(batch.get(pointer));
					pointer++;
					return newTuple;
				}
			}
		}
		else{
			ArrayList<LeafValue> newTuple = new ArrayList<LeafValue>(tuple);
			newTuple.addAll(batch.get(pointer));
			pointer++;
			return newTuple;
		}
	}

	@Override
	public void reset() {
		schema = new ArrayList<Column>();
		batch = new ArrayList<ArrayList<LeafValue>>();
		oper.reset();
		schema = oper.getSchema();
		schema.addAll(operJoin.getSchema());
	}

	@Override
	public void dump() {
		Output.dump(this);
	}

	@Override
	public ArrayList<Column> getSchema() {
		return schema;
	}
}