package com.myDB;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;

/*

This class performs all the aggregation operations
It aggregation type:int determines what function to perform
It then takes two leaf values and performs the aggregation operation on it

*/

public class Functions {
	private int type;
	private LeafValue result;
	private LeafValue latest;

	public Functions(int type) {
		this.type = type;
	}
	
	public int getType(){
		return type;
	}
	
	public void accept(LeafValue result, LeafValue latest){
		this.result = result;
		this.latest = latest;
		switch(type){
			case 0 : count(); break;
			case 1 : sum(); break;
			case 2 : avg(); break;
			case 3 : min(); break;
			case 4 : max(); break;
		}
	}
	
	public LeafValue receive(){
		return result;
	}
	
	private void count(){
		try {
			result = new LongValue(result.toLong() + 1);
		} catch (InvalidLeaf e) {}			
	}
	
	private void sum(){
		double temp = 0;
		try {
			temp = result.toDouble() + latest.toDouble();
			result = new DoubleValue(temp);
		} catch (InvalidLeaf e) {}
	}
	
	private void avg(){
		sum();
	}
	
	private void min(){
		try{
			if(result.toDouble() > latest.toDouble()) result = latest;
		} catch (InvalidLeaf e) {}
	}
	
	private void max(){
		try{
			if(result.toDouble() < latest.toDouble()) result = latest;
		} catch (InvalidLeaf e) {}
	}
}