package com.myDB;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/*

Class that extens the Eval class from Evaluation Library
The eval method takes the column to perform the evaluation
It takes an expression and determines it value for the given list of tuple by solving the expression

*/

public class ConditionVisitor extends Eval{
	@SuppressWarnings("unused")
	private LeafValue leaf = null;
	private ArrayList<Column> schema;
	private ArrayList<LeafValue> tuple;
	private ArrayList<SelectExpressionItem> alias = new ArrayList<SelectExpressionItem>();
	private HashMap<String, Integer> map = new HashMap<String, Integer>();
	
	public ConditionVisitor(ArrayList<Column> schema, ArrayList<SelectExpressionItem> alias) {
		this.schema = schema;
		if(alias != null) this.alias = alias;
	}
	
	public void setTuple(ArrayList<LeafValue> tuple){
		this.tuple = tuple;
	}

	// Function that takes an expression and solves it for the current tuple
	@Override
	public LeafValue eval(Column column){
		try{
			String colName = column.getColumnName();
			String tableName = column.getTable().getName();
			String name = tableName;
			if(name == null) name = "";
			Integer val = map.get(name + "." + colName);
			if(val == null){
				int index = -1;
				for(int i=0; i<alias.size(); i++){
					if(alias.get(i).getAlias().equals(colName)){
						Expression expr = alias.get(i).getExpression();
						String exprStr = expr.toString();
						if(exprStr.contains("(") == true){
							return this.eval(expr);
						}
						String[] arr = exprStr.split("\\.");
						if(arr.length == 2){
							colName = arr[1];
							tableName = arr[0];
						}
						else colName = arr[0];
						break;
					}
				}
				for(int i=0; i<schema.size(); i++){
					if(schema.get(i).getColumnName().toLowerCase().equals(colName.toLowerCase())){
						if(tableName != null){
							if(schema.get(i).getTable().getName().toLowerCase().equals(tableName.toLowerCase())){
								index = i;
								break;	
							}
							else if(schema.get(i).getTable().getAlias() != null && schema.get(i).getTable().getAlias().toLowerCase().equals(tableName.toLowerCase())){
								index = i;
								break;	
							}
						}
						else{
							index = i;
							break;
						}
					}
				}
				map.put(name + "." + colName, index);
				leaf = tuple.get(index); 
				return tuple.get(index);
			}
			else{
				leaf = tuple.get(val); 
				return tuple.get(val);
			}
		}
		catch(Exception e){
			System.out.println(column);
			System.out.println(schema);
			System.out.println("Parse error has occurred.");
			return new LongValue(0);
		}
	}
}