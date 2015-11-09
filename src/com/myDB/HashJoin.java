package com.myDB;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

/*

Class that implements HashJoin for faster joins

*/

public class HashJoin implements Operator {
	private Operator oper;
	private Operator operJoin;
	private ArrayList<Column> schema;
	private String alias;
	private ArrayList<Integer> leftIndex = new ArrayList<Integer>();
	private ArrayList<Integer> rightIndex = new ArrayList<Integer>();
	private HashMap<String, ArrayList<ArrayList<LeafValue>>> map;
	private ArrayList<ArrayList<LeafValue>> batch;
	private int pointer = 0;
	private int buffer = -1;
	private ArrayList<LeafValue> tuple = null;
	private ArrayList<Expression> wList = new ArrayList<Expression>();
	
	
	// Initialize the join operations and create new schema.
	public HashJoin(Operator oper, Operator operJoin, String alias, Expression condition){
		this.oper = oper;
		this.operJoin = operJoin;
		this.alias = alias;
		whereExpr(condition);
		for(Expression expr : wList){
			String left = ((BinaryExpression) expr).getLeftExpression().toString();
			String right = ((BinaryExpression) expr).getRightExpression().toString();
			ArrayList<Column> schema = oper.getSchema();
			for(int i=0; i<schema.size(); i++){
				String strT = schema.get(i).getTable().getName() + "." + schema.get(i).getColumnName();
				String strA = schema.get(i).getTable().getAlias() + "." + schema.get(i).getColumnName();
				if(left.equals(strT) || left.equals(strA)){
					leftIndex.add(i);
					break;
				}
				else if(right.equals(strT) || right.equals(strA)){
					leftIndex.add(i);
					break;
				}
			}
			schema = null;
			schema = operJoin.getSchema();
			for(int i=0; i<schema.size(); i++){
				String strT = schema.get(i).getTable().getName() + "." + schema.get(i).getColumnName();
				String strA = schema.get(i).getTable().getAlias() + "." + schema.get(i).getColumnName();
				if(left.equals(strT) || left.equals(strA)){
					rightIndex.add(i);
					break;
				}
				else if(right.equals(strT) || right.equals(strA)){
					rightIndex.add(i);
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
	
	// Create the HashMap by making join clause as Key and rest of the tuple as value.
	public void fetchData(){
		ArrayList<LeafValue> tuple;
		while((tuple=oper.readOneTuple()) != null){
			String key = "";
			for(int index : leftIndex) key += tuple.get(index).toString() + "|";
			key = key.substring(0, key.length()-1);
			ArrayList<ArrayList<LeafValue>> batch = map.get(key);
			if(batch != null) batch.add(tuple);
			else{
				batch = new ArrayList<ArrayList<LeafValue>>();
				batch.add(tuple);
			}
			map.put(key, batch);
		}
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() { // Match the join clause from the second operator to the key in HashMap & return the merged tuple
		if(buffer == -1 || pointer == buffer){
			while(true){
				tuple = operJoin.readOneTuple();
				if(tuple == null) return null;
				String key = "";
				pointer = 0;
				buffer = -1;
				for(int index : rightIndex) key += tuple.get(index).toString() + "|";
				key = key.substring(0, key.length()-1);
				batch = map.get(key);
				if(batch != null){
					buffer = batch.size();
					ArrayList<LeafValue> newTuple = new ArrayList<LeafValue>(batch.get(pointer));
					pointer++;
					newTuple.addAll(tuple);
					return newTuple;
				}
			}
		}
		else{
			ArrayList<LeafValue> newTuple = new ArrayList<LeafValue>(batch.get(pointer));
			pointer++;
			newTuple.addAll(tuple);
			return newTuple;
		}
	}

	@Override
	public void reset() {
		schema = new ArrayList<Column>();
		batch = new ArrayList<ArrayList<LeafValue>>();
		map = new HashMap<String, ArrayList<ArrayList<LeafValue>>>();
		this.oper.reset();
		this.operJoin.reset();
		schema = oper.getSchema();
		if(alias != null){
			Operator operTemp = new Schema(operJoin, alias);
			schema.addAll(operTemp.getSchema());
		}
		else{
			schema.addAll(operJoin.getSchema());
		}
		fetchData();
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