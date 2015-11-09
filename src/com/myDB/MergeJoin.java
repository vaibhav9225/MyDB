package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Column;

/*

Class to perform merge join between two operators.

*/

public class MergeJoin implements Operator {
	private Operator oper;
	private Operator operJoin;
	private ArrayList<Column> schema;
	private String alias;
	private ArrayList<LeafValue> leftTup = null;
	private ArrayList<LeafValue> rightTup = null;
	private int leftIndex;
	private int rightIndex;
	
	// Initialize schema.
	public MergeJoin(Operator oper, Operator operJoin, String alias, Expression condition){
		this.oper = oper;
		this.operJoin = operJoin;
		this.alias = alias;
		String left = ((BinaryExpression) condition).getLeftExpression().toString();
		String right = ((BinaryExpression) condition).getRightExpression().toString();
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
		schema = null;
		schema = operJoin.getSchema();
		for(int i=0; i<schema.size(); i++){
			String strT = schema.get(i).getTable().getName() + "." + schema.get(i).getColumnName();
			String strA = schema.get(i).getTable().getAlias() + "." + schema.get(i).getColumnName();
			if(left.equals(strT) || left.equals(strA)){
				rightIndex = i;
				break;
			}
			else if(right.equals(strT) || right.equals(strA)){
				rightIndex = i;
				break;
			}
		}
		reset();
	}
	
	// Function that emulates merge operation in merge Sort. Used only to merge on sorted numeric values.
	@Override
	public ArrayList<LeafValue> readOneTuple() {
		try {
			if(leftTup == null){
				leftTup = oper.readOneTuple();
				rightTup = operJoin.readOneTuple();
			}
			while(true){
				if(leftTup == null) return null;
				if(rightTup == null) return null;
				long leftVal = leftTup.get(leftIndex).toLong();
				long rightVal = rightTup.get(rightIndex).toLong();
				if(leftVal < rightVal) leftTup = oper.readOneTuple();
				else if(leftVal > rightVal) rightTup = operJoin.readOneTuple();
				else{
					leftTup.addAll(rightTup);
					ArrayList<LeafValue> returnVal = leftTup;
					leftTup = oper.readOneTuple();
					return returnVal;
				}
			}
		} catch (InvalidLeaf e) {}
		return null;
	}

	@Override
	public void reset() {
		schema = new ArrayList<Column>();
		leftTup = null;
		rightTup = null;
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