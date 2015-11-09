package com.myDB;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

/*

Class that implements block join operation.

*/

public class BlockJoin implements Operator {
	private Operator oper;
	private Operator operJoin;
	private int operCount;
	private boolean operFlag;
	private int operJoinCount;
	private Iterator<ArrayList<LeafValue>> operIter;
	private Iterator<ArrayList<LeafValue>> operJoinIter;
	private ArrayList<Column> schema;
	private ArrayList<ArrayList<LeafValue>> operList;
	private ArrayList<ArrayList<LeafValue>> operJoinList;
	private final int COUNT = 1000;
	private ArrayList<LeafValue> tuple;
	private String alias;
	
	public BlockJoin(Operator oper, Operator operJoin, String alias){
		this.oper = oper;
		this.operJoin = operJoin;
		this.alias = alias;
		reset();
	}
	
	// Fetch a block of data from both operators.
	public boolean fetchData(){
		ArrayList<LeafValue> tuple = null;
		try{
			if(operFlag == false){
				operJoin.reset();
				operList = new ArrayList<ArrayList<LeafValue>>();
				operJoinList = new ArrayList<ArrayList<LeafValue>>();
				while(operCount < COUNT && (tuple=oper.readOneTuple()) != null){
					operList.add(tuple);
					operCount++;
				}
				operFlag = true;
				operCount = 0;
			}
			operJoinList = new ArrayList<ArrayList<LeafValue>>();
			while(operJoinCount < COUNT && (tuple=operJoin.readOneTuple()) != null){
				operJoinList.add(tuple);
				operJoinCount++;
			}
			operIter = operList.iterator();
			operJoinIter = operJoinList.iterator();
			if(tuple == null) operFlag = false;
			operJoinCount = 0;
			if(operJoinList.size() == 0) return fetchData();
			if(operList.size()==0 && operJoinList.size()>0) return false;
			return true;
		}
		catch(NoSuchElementException e){
			return false;
		}
	}

	@Override
	public ArrayList<LeafValue> readOneTuple() { // Iterate over both blocks to read one tuple at a time.
		ArrayList<LeafValue> tempTuple = null;
		if(operIter.hasNext() || operJoinIter.hasNext()){
			if(tuple == null) tuple = new ArrayList<LeafValue>(operIter.next());
			if(operJoinIter.hasNext()){
				tempTuple = new ArrayList<LeafValue>(tuple);
				tempTuple.addAll(operJoinIter.next());
			}
			else{
				tuple = null;
				operJoinIter = operJoinList.iterator();
				return readOneTuple();
			}
		}
		else{
			tuple = null;
			if(fetchData()){
				return readOneTuple();
			}
			else return null;
		}
		return tempTuple;
	}

	@Override
	public void reset() {
		operCount = 0;
		operFlag = false;
		operJoinCount = 0;
		operIter = null;
		operJoinIter = null;
		schema = new ArrayList<Column>();
		operList = new ArrayList<ArrayList<LeafValue>>();
		operJoinList = new ArrayList<ArrayList<LeafValue>>();
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
		tuple = null;
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