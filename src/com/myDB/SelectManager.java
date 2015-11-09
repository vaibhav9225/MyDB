package com.myDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;

/*

Class that parsers a single query statement.
Identifies the operations which needs to be performed.
Creates and calls the respective operator classes.

*/

public class SelectManager{
	private Operator oper;
	private HashMap<Integer, Integer> fn = new HashMap<Integer, Integer>();
	@SuppressWarnings("rawtypes")
	private List items;
	private ArrayList<SelectExpressionItem> alias = new ArrayList<SelectExpressionItem>();
	private ArrayList<Expression> wList = new ArrayList<Expression>();
	
	public  ArrayList<Expression> getList(){
		return wList;
	}
	
	public void setList(ArrayList<Expression> wList){
		this.wList = new ArrayList<Expression>(wList);
	}

	@SuppressWarnings("rawtypes")
	public SelectManager(PlainSelect select, Metadata mData){
		//Setup alias
		items = select.getSelectItems();
		for(Object column : items){
			if(column instanceof SelectExpressionItem){
				if(((SelectExpressionItem) column).getAlias() != null){
					alias.add((SelectExpressionItem) column);
				}
			}
		}
		//Setup alias
		Expression whereExpr = select.getWhere();
		whereExpr(whereExpr);
		FromItem fromItem = (FromItem) select.getFromItem();
		FromVisitor fromVisitor = new FromVisitor(mData, this);
		fromItem.accept(fromVisitor);
		oper = fromVisitor.getOperator();
		if(select.getJoins() != null){ // Fetch data from each tables and perform join operation on them.
			for(Object join : select.getJoins()){
				SubJoin subJoin = new SubJoin();
				subJoin.setJoin((Join) join);
				subJoin.setLeft(fromItem);
				subJoin.accept(fromVisitor);
			}
			oper = fromVisitor.getOperator();
		}
		if(whereExpr != null){ // Filter data by performing where operation.
			for(Expression expr : wList){
				oper = new Selection(oper, expr, alias);
			}
		}
		oper = new Projection(oper, items, fn);
		List groupBy = select.getGroupByColumnReferences();
		if(groupBy != null || fn.size() > 0){ // Perform aggregation operation.
			oper = new Aggregation(oper, groupBy, alias, fn);
			if(select.getHaving() != null){
				oper = new Selection(oper, select.getHaving(), alias);
			}
		}
		if(select.getOrderByElements() != null){ // Order the elements based on ORDER BY clauses.
			oper = new Sort(oper, select.getOrderByElements(), alias);
		}
		if(select.getLimit() != null){ // Limit the number of rows to be returned.
			oper = new Limit(oper, select.getLimit().getRowCount());
		}
	}
	
	
	// Funtion to identify all where expressions.
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

	public void close() {
		System.gc();
	}
	
	public Operator getOperator(){
		return oper;
	}
}