package com.myDB;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;

// Read one tuple at a time and print in to console.
public class Output {
	public static void dump(Operator oper){
		ArrayList<LeafValue> lValues;
		while((lValues=oper.readOneTuple()) != null){
			String tuple = "";
			for(LeafValue lValue : lValues){
				String str;
				if(lValue == null) str = "";
				else str = lValue.toString();
				if(lValue instanceof StringValue) tuple += str.substring(1, str.length()-1) + "|";
				else tuple += str + "|";
			}
			System.out.println(tuple.substring(0, tuple.length()-1));
		}
	}
}
