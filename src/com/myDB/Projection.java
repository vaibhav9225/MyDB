package com.myDB;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/*

Class that implements the Projection Operator.
Filters all the columns that are specified in the SELECT clause and evaluates the expression.

*/

public class Projection implements Operator{
	private Operator oper;
	@SuppressWarnings("rawtypes")
	private List items;
	private ArrayList<Column> schema;
	private HashMap<Integer, Expression> map;
	private HashMap<Integer, Integer> fn;
	public ArrayList<Integer> indices;
	private boolean allColumns;
	private ConditionVisitor selectEval;

	@SuppressWarnings("rawtypes")
	public Projection(Operator oper, List items, HashMap<Integer, Integer> fn) {
		this.oper = oper;
		this.items = items;
		this.fn = fn;
		this.init();
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() { // Read one tuple at a time.
		ArrayList<LeafValue> tuple = oper.readOneTuple();
		selectEval = new ConditionVisitor(oper.getSchema(), null);
		if(tuple != null){
			if(allColumns == true) return tuple;
			else{
				ArrayList<LeafValue> tempTuple = new ArrayList<LeafValue>();
				for(int i=0; i<indices.size(); i++){
					if(indices.get(i) != -1) tempTuple.add(tuple.get(indices.get(i)));
					else{
						selectEval.setTuple(tuple);
						Expression expr = map.get(i);
						if(expr instanceof Function){ // Evaluate the function in the SELECT clause
							Function fun = (Function) expr;
							LeafValue evalLeaf = new LongValue(1);
							if(fun.isAllColumns()) evalLeaf = new LongValue(1);
							else{
								Expression subExpr = ((Expression)(fun.getParameters().getExpressions().get(0)));
								try {
									evalLeaf = selectEval.eval(subExpr);
								} catch (SQLException e) {}
							}
							tempTuple.add(evalLeaf);
						}
						else{ // Else simple push to the tuple list.
							LeafValue evalLeaf;
							try {
								evalLeaf = selectEval.eval(expr);
								tempTuple.add(evalLeaf);
							} catch (SQLException e) {}
						}
					}
				}
				return tempTuple;
			}
		}
		return null;
	}

	@Override
	public ArrayList<Column> getSchema() {
		return schema;
	}
	
	private void init(){
		schema = new ArrayList<Column>();
		map = new HashMap<Integer, Expression>();
		indices = new ArrayList<Integer>();
		allColumns = false;
		for(Object column : items){
			if(column instanceof AllColumns){ // If all coulumns, return all the values in tuple.
				schema.addAll(oper.getSchema());
				allColumns = true;
				break;
			}
			else if(column instanceof AllTableColumns){ // If all table coulumns, return all values in tuple related to that table.
				String[] arr = ((AllTableColumns) column).toString().split("\\.");
				String tableName = arr[0];
				ArrayList<Column> tuple = new ArrayList<Column>(oper.getSchema());
				for(int i=0; i<tuple.size(); i++){
					if(tuple.get(i).getTable().getName() != null && tuple.get(i).getTable().getName().equals(tableName)){
						schema.add(tuple.get(i));
						indices.add(i);
					}
					else if(tuple.get(i).getTable().getAlias() != null && tuple.get(i).getTable().getAlias().equals(tableName)){
						schema.add(tuple.get(i));
						indices.add(i);
					}
				}
			}
			else if(column instanceof SelectExpressionItem){ // Else identify the expression and evaluate it.
				Expression colExpr = ((SelectExpressionItem) column).getExpression();
				String col = colExpr.toString();
				String alias = ((SelectExpressionItem) column).getAlias();
				String[] arr = col.split("\\.");
				String colName = arr[0];
				String tableName = null;
				if(arr.length == 2){
					colName = arr[1];
					tableName = arr[0];
				}				
				ArrayList<Column> tuple = new ArrayList<Column>(oper.getSchema());
				int prevSize = schema.size();
				for(int i=0; i<tuple.size(); i++){
					if(tuple.get(i).getColumnName().equals(colName)){
						if(tableName != null){
							if(tuple.get(i).getTable().getName() != null && tuple.get(i).getTable().getName().equals(tableName)){
								if(alias != null) tuple.get(i).setColumnName(alias);
								schema.add(tuple.get(i));
								indices.add(i);
								break;
							}
							else if(tuple.get(i).getTable().getAlias() != null && tuple.get(i).getTable().getAlias().equals(tableName)){
								if(alias != null) tuple.get(i).setColumnName(alias);
								schema.add(tuple.get(i));
								indices.add(i);
								break;
							}
						}
						else{
							schema.add(tuple.get(i));
							indices.add(i);
							break;
						}
					}
				}
				int nextSize = schema.size();
				if(prevSize == nextSize){
					Table table = new Table();
					Column newCol;
					if(alias == null) newCol = new Column(table, col);
					else newCol = new Column(table, alias);
					schema.add(newCol);
					indices.add(-1);
					int index = indices.size() - 1;
					map.put(index, colExpr);
				}
			}
		}
		for(int i=0; i<indices.size(); i++){
			Expression expr = map.get(i);
			if(expr instanceof Function){
				Function fun = (Function) expr;
				int index = 0;
				switch(fun.getName().toLowerCase()){
				case "count" : index = 0; break;
				case "sum" : index = 1; break;
				case "avg" : index = 2; break;
				case "min" : index = 3; break;
				case "max" : index = 4; break;
				}
				fn.put(i, index);
			}
		}
	}

	@Override
	public void reset() {
		oper.reset();
		this.init();
	}

	@Override
	public void dump() {
		Output.dump(this);
	}
}