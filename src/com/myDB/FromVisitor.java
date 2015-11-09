package com.myDB;

import java.io.StringReader;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

/*

Class that uses visitor design pattern to check if the statement is of type TABLE|JOIN|SUBJOIN

*/

@SuppressWarnings("static-access")
public class FromVisitor implements FromItemVisitor{
	private static Metadata mData = null;
	private Operator oper = null;
	private Operator operJoin = null;
	private ArrayList<Expression> wList = new ArrayList<Expression>();
	private Expression expr = null;
	private String exprString  = "";
	private ArrayList<String> exprArr = new ArrayList<String>();
	private Expression exprJoin = null;
	private String exprStringJoin  = "";
	private ArrayList<String> exprArrJoin = new ArrayList<String>();
	private SelectManager manager;
	private ArrayList<String> relations = new ArrayList<String>(); 

	public FromVisitor(Metadata mData, SelectManager manager) {
		this.mData = mData;
		this.wList = manager.getList();
		this.manager = manager;
	}

	@Override
	public void visit(Table table) {
		if(!relations.contains(table.toString())) relations.add(table.toString());
		oper = new Relation(table, mData);
		expr = null;
		exprString  = "";
		exprArr = new ArrayList<String>();
		if(expr == null){
			if(!wList.isEmpty()){
				ArrayList<Expression> tempList = new ArrayList<Expression>(wList);
				for(Expression expr : wList){
					if(expr instanceof BinaryExpression || expr instanceof Parenthesis){
						if(expr instanceof Parenthesis){
							if(expr.toString().contains(table.getName())){
								exprArr.add(expr.toString());
								tempList.set(wList.indexOf(expr), null);
							}
						}
						else{
							if(!(((BinaryExpression) expr).getLeftExpression() instanceof Column && ((BinaryExpression) expr).getRightExpression() instanceof Column)){
								if(((BinaryExpression) expr).getLeftExpression().toString().split("\\.")[0].toLowerCase().contains(table.getName().toLowerCase())){
									exprArr.add(expr.toString());
									tempList.set(wList.indexOf(expr), null);
								}
							}
							else if(((BinaryExpression) expr).getLeftExpression() instanceof Column && ((BinaryExpression) expr).getRightExpression() instanceof Column){
								if(((BinaryExpression) expr).getLeftExpression().toString().split("\\.")[0].toLowerCase().contains(table.getName().toLowerCase()) && ((BinaryExpression) expr).getRightExpression().toString().split("\\.")[0].toLowerCase().contains(table.getName().toLowerCase())){
									exprArr.add(expr.toString());
									tempList.set(wList.indexOf(expr), null);
								}
							}
						}
					}
				}
				ArrayList<Expression> newTempList = new ArrayList<Expression>();
				if(!exprArr.isEmpty()){
					for(Expression e : tempList){
						if(e != null) newTempList.add(e);
					}
					wList = new ArrayList<Expression>(newTempList);
					manager.setList(wList);
				}
			}
			for(int i=0; i<exprArr.size(); i++){
				if(i == exprArr.size()-1) exprString += exprArr.get(i);
				else  exprString += exprArr.get(i) + " AND ";
			}
			if(exprString != ""){
				exprString = "SELECT * FROM DATAMINIONS WHERE " + exprString;
				CCJSqlParser parser = new CCJSqlParser(new StringReader(exprString));
				try {
					PlainSelect select = (PlainSelect) (((Select) parser.Statement()).getSelectBody());
					expr = select.getWhere();
				}
				catch (ParseException e) {}
			}
		}
		if(expr != null){
			oper = new Selection(oper, expr, null);
			oper = new Compressor(oper, table.getName());
		}
	}
	
	public Expression getExpr(){
		return expr;
	}

	@Override
	public void visit(SubSelect subSelect) {
		SelectManager subSelectManager = new SelectManager((PlainSelect) subSelect.getSelectBody(), mData);
		oper = subSelectManager.getOperator();
		String alias = null;
		if(subSelect.getAlias() != null){
			alias = subSelect.getAlias();
			oper = new Schema(oper, alias);
		}
		subSelectManager.close();
	}

	@Override
	public void visit(SubJoin subJoin) { // JOIN OPERATION - Create two seperate operators for each table/
		Expression prevExpr = null;
		Join join = subJoin.getJoin();
		String relationText = join.getRightItem().toString();
		String alias = null;
		if(relationText.contains("select") || relationText.contains("SELECT") || relationText.contains("Select")){
			relationText = relationText.trim();
			relationText = relationText.substring(1, relationText.length()-1);
			CCJSqlParser parser = new CCJSqlParser(new StringReader(relationText));
			try {
				Statement subStmt = parser.Statement();
				StatementManager subStmtManager = new StatementManager((Select) subStmt, mData);
				operJoin = subStmtManager.select();
				if(join.getRightItem().getAlias() != null){
					alias = join.getRightItem().getAlias();
					if(!relations.contains(alias)) relations.add(alias);
				}
				subStmtManager.close();
			}
			catch (ParseException e) {
				System.out.println("Invalid SubJoin");
			}
		}
		else{
			if(!relations.contains(relationText)) relations.add(relationText);
			FromVisitor subFromVisitor = new FromVisitor(mData, manager);
			join.getRightItem().accept(subFromVisitor);
			operJoin = subFromVisitor.getOperator();
			prevExpr = subFromVisitor.getExpr();
		}
		wList = manager.getList();
		exprJoin = null;
		exprStringJoin  = "";
		exprArrJoin = new ArrayList<String>();
		if(exprJoin == null){
			if(!wList.isEmpty()){
				ArrayList<Expression> tempList = new ArrayList<Expression>(wList);
				for(Expression expr : wList){
					if(expr instanceof BinaryExpression){
						if(((BinaryExpression) expr).getLeftExpression() instanceof Column && ((BinaryExpression) expr).getRightExpression() instanceof Column){
							boolean lf = false;
							boolean rt = false;
							for(String relation : relations){
								if(((BinaryExpression) expr).getLeftExpression().toString().split("\\.")[0].toLowerCase().contains(relation.toLowerCase())) lf = true;
								else if(((BinaryExpression) expr).getRightExpression().toString().split("\\.")[0].toLowerCase().contains(relation.toLowerCase())) rt = true;
							}
							if(lf == true && rt == true){
								exprArrJoin.add(expr.toString());
								tempList.set(wList.indexOf(expr), null);
							}
						}
					}
				}
				ArrayList<Expression> newTempList = new ArrayList<Expression>();
				if(!exprArrJoin.isEmpty()){
					for(Expression e : tempList){
						if(e != null) newTempList.add(e);
					}
					wList = new ArrayList<Expression>(newTempList);
					manager.setList(wList);
				}
			}
			for(int i=0; i<exprArrJoin.size(); i++){
				if(i == exprArrJoin.size()-1) exprStringJoin += exprArrJoin.get(i);
				else  exprStringJoin += exprArrJoin.get(i) + " AND ";
			}
			if(exprStringJoin != ""){
				exprStringJoin = "SELECT * FROM DATAMINIONS WHERE " + exprStringJoin;
				CCJSqlParser parser = new CCJSqlParser(new StringReader(exprStringJoin));
				try {
					PlainSelect select = (PlainSelect) (((Select) parser.Statement()).getSelectBody());
					exprJoin = select.getWhere();
				}
				catch (ParseException e) {}
			}
		}
		if(exprJoin != null){
			// TEMP CODE
			if(Code.getType() == 5 || Code.getType() == 6){
				if(relationText.equals("orders")){ // If index present, use indexed hash join.
					oper = new IndexHashJoin(oper, new IndexedRelation(relationText, prevExpr), exprJoin);
				}
				else{ // Else use hash join.
					if(alias != null) oper = new HashJoin(oper, operJoin, alias, exprJoin);
					else oper = new HashJoin(oper, operJoin, null, exprJoin);
				}
			}
			else{
			// TEMP CODE
			if(alias != null) oper = new HashJoin(oper, operJoin, alias, exprJoin);
			else oper = new HashJoin(oper, operJoin, null, exprJoin);
			// TEMP CODE
			}
			// TEMP CODE
		}
		else{
			if(alias != null) oper = new BlockJoin(oper, operJoin, alias);
			else oper = new BlockJoin(oper, operJoin, null);
		}
		Expression onExpr = join.getOnExpression();
		if(onExpr != null) oper = new Selection(oper, onExpr, null);
	}
	
	public Operator getOperator(){
		return oper;
	}
}