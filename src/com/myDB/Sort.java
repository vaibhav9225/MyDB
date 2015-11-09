package com.myDB;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/*

Operator to emulate ORDER BY

*/

class Sort implements Operator{
	private Operator oper;
	private LinkedList<ArrayList<LeafValue>> tupleList;
	private int index;
	@SuppressWarnings("rawtypes")
	private List elements;
	private ArrayList<Integer> indices;
	private ArrayList<Boolean> bools;
	private ArrayList<SelectExpressionItem> alias = new ArrayList<SelectExpressionItem>();
	
	@SuppressWarnings("rawtypes")
	public Sort(Operator oper, List elements, ArrayList<SelectExpressionItem> alias) {
		this.oper = oper;
		this.elements = elements;
		if(alias != null) this.alias = new ArrayList<SelectExpressionItem>(alias);
		init();
	}
	
	// Initialize schema.
	private void init(){
		tupleList = new LinkedList<ArrayList<LeafValue>>();
		indices = new ArrayList<Integer>();
		bools = new ArrayList<Boolean>();
		index = 0;
		for(Object obj : elements){
			bools.add(((OrderByElement) obj).isAsc());
			String[] array = ((OrderByElement) obj).getExpression().toString().split("\\.");
			String colName = null;
			String tableName = null;
			if(array.length == 2){
				colName = array[1];
				tableName = array[0];
			}
			else{
				colName = array[0];
			}
			ArrayList<Column> schema = oper.getSchema();
			for(int i=0; i<alias.size(); i++){
				if(alias.get(i).getAlias().equals(colName)){
					Expression expr = alias.get(i).getExpression();
					String exprStr = expr.toString();
					if(exprStr.contains("(") != true){
						String[] arr = exprStr.split("\\.");
						if(arr.length == 2){
							colName = arr[1];
							tableName = arr[0];
						}
						else colName = arr[0];
						break;
					}
					else{
						colName = alias.get(i).getAlias();
						break;
					}
				}
			}
			for(int i=0; i<schema.size(); i++){
				if(schema.get(i).getColumnName().toLowerCase().equals(colName.toLowerCase())){
					if(tableName != null){
						if(schema.get(i).getTable().getName().toLowerCase().equals(tableName.toLowerCase())){
							indices.add(i);
							break;	
						}
						else if(schema.get(i).getTable().getAlias().toLowerCase().equals(tableName.toLowerCase())){
							indices.add(i);
							break;	
						}
					}
					else{
						indices.add(i);
						break;
					}
				}
			}
		}
		
		// Fetch all the tuples.
		ArrayList<LeafValue> tuple;
		while((tuple=oper.readOneTuple()) != null){
			tupleList.add(tuple);
		}
		int prevIndex = -1;
		// Create an algorithm object to Sort the data.
		for(int i=0; i<indices.size(); i++){
			Algorithm sort = new Algorithm();
			sort.sort(tupleList, indices.get(i), prevIndex, bools.get(i), 0);
			prevIndex = indices.get(i);
		}
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() {
		if(index < tupleList.size()){
			index++;
			return tupleList.get(index-1);
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
		init();
	}

	@Override
	public void dump() {
		Output.dump(this);
	}
}