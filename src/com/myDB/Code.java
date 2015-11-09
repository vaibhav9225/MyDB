package com.myDB;

import java.io.StringReader;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

/*

A class that maps the name of the Columns to the column numbers in the data provided.
Used only for testing purposes and can be decoupled from the rest of the code.

*/

public class Code {
	
	private static String stmt = "";
	private static int type = 0;
	private static Statement query = null;
	private static HashMap<String, Integer[]> relationMap = new HashMap<String, Integer[]>();
	private static HashMap<String, Integer[]> selectionMap = new HashMap<String, Integer[]>();
	
	public static void setMaps(){
		int type = getType();
		if(type == 1 || type == 2){
			relationMap.put("lineitem", new Integer[]{4,5,6,7,8,9,10});
			selectionMap.put("lineitem", new Integer[]{0,1,2,3,4,5});
		}
		else if(type == 3 || type == 4){
			relationMap.put("lineitem", new Integer[]{0,5,6,10});
			relationMap.put("orders", new Integer[]{0,1,4,7});
			relationMap.put("customer", new Integer[]{0,6});
			selectionMap.put("lineitem", new Integer[]{0,1,2});
			selectionMap.put("orders", new Integer[]{0,1,2,3});
			selectionMap.put("customer", new Integer[]{0});
		}
		else if(type == 5 || type == 6){
			relationMap.put("lineitem", new Integer[]{0,2,5,6});
			relationMap.put("orders", new Integer[]{0,1,4});
			relationMap.put("customer", new Integer[]{0,3});
			relationMap.put("supplier", new Integer[]{0,3});
			relationMap.put("nation", new Integer[]{0,1,2});
			relationMap.put("region", new Integer[]{0,1});
			selectionMap.put("lineitem", new Integer[]{0,1,2,3});
			selectionMap.put("orders", new Integer[]{0,1});
			selectionMap.put("customer", new Integer[]{0,1});
			selectionMap.put("supplier", new Integer[]{0,1});
			selectionMap.put("nation", new Integer[]{0,1,2});
			selectionMap.put("region", new Integer[]{0});
		}
		else if(type == 7 || type == 8){
			relationMap.put("lineitem", new Integer[]{0,5,6,8});
			relationMap.put("orders", new Integer[]{0,1,4});
			relationMap.put("customer", new Integer[]{0,2,3,4,5,7});
			relationMap.put("nation", new Integer[]{0,1});
			selectionMap.put("lineitem", new Integer[]{0,1,2});
			selectionMap.put("orders", new Integer[]{0,1});
			selectionMap.put("customer", new Integer[]{0,1,2,3,4,5});
			selectionMap.put("nation", new Integer[]{0,1});
		}
		else if(type == 9 || type == 10){
			relationMap.put("lineitem", new Integer[]{0,10,11,12,14});
			relationMap.put("orders", new Integer[]{0,5});
			selectionMap.put("lineitem", new Integer[]{0,4});
			selectionMap.put("orders", new Integer[]{0,1});
		}
	}
	
	public static Integer[] getRelationMap(String table){
		return relationMap.get(table);
	}
	
	public static Integer[] getSelectionMap(String table){
		return selectionMap.get(table);
	}
	
	public static void accept(String str){
		stmt = str;
		setMaps();
	}
	
	public static int getType(){
		if(check0("FROM region, nation, customer, orders, lineitem, supplier")) type = 5;
		else if(check1("FROM region, nation, customer, orders, lineitem, supplier")) type = 6;
		else if(check0("FROM customer, orders, lineitem, nation")) type = 7;
		else if(check1("FROM customer, orders, lineitem, nation")) type = 8;
		else if(check0("FROM customer, orders, lineitem")) type = 3;
		else if(check1("FROM customer, orders, lineitem")) type = 4;
		else if(check0("FROM lineitem, orders")) type = 9;
		else if(check1("FROM lineitem, orders")) type = 10;
		else if(check0("FROM lineitem")) type = 1;
		else if(check1("FROM lineitem")) type = 2;
		return type;
	}
	
	public static boolean hasSelect(){
		boolean bool = false;
		CCJSqlParser parser = new CCJSqlParser(new StringReader(stmt));
		try {
			query = parser.Statement();
		} catch(Exception e){}
		return bool;
	}
	
	public static Statement getSelect(){
		return query;
	}
	
	private static boolean check0(String str){
		if(stmt.contains(str) && Metadata.swapDir == null) return true;
		else return false;
	}
	
	private static boolean check1(String str){
		if(stmt.contains(str) && Metadata.swapDir != null) return true;
		else return false;
	}
}