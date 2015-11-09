package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

// Class that performs different operations.

@SuppressWarnings("static-access")
public class StatementManager {
	private SelectManager manager = null;
	private static Metadata mData = null;
	private Select select = null;
	private Operator oper = null;
	ArrayList<Operator> opers = new ArrayList<Operator>();
	
	public StatementManager(Select select, Metadata mData) {
		this.mData = mData;
		this.select = select;
	}
	
	public Operator select(){
		SelectBody selectBody = select.getSelectBody();
		if(selectBody instanceof PlainSelect){ // If simple SELECT, then perform it.
			PlainSelect plainSelect = (PlainSelect) selectBody;
			mData.debugAll("\nQuerying: " + plainSelect.toString() + "\n");
			manager = new SelectManager(plainSelect, mData);
			oper = manager.getOperator();
		}
		else if(selectBody instanceof Union){ // If UNION, then perform all simple select and merge the results.
			Union union = (Union) selectBody;
			mData.debugAll("Querying: " + union.toString());
			for(int i=0; i<union.getPlainSelects().size(); i++){
				manager = new SelectManager((PlainSelect)union.getPlainSelects().get(i), mData);
				oper = manager.getOperator();
				opers.add(oper);
			}
			oper = new Combine(opers);
		}
		manager.close();
		return oper;
	}

	public void close() {
		System.gc();
	}
}