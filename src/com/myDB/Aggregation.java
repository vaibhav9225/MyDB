package com.myDB;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/*

Class that performs the GROUP BY operation in the query
The constructor takes the current operator and and the list og GROUP BY clauses
Agrregates the values by pushing the GROUP BY clause as key in HashMap

*/

class Aggregation implements Operator{
	private Operator oper;
	private HashMap<String, ArrayList<LeafValue>> map;
	private HashMap<Integer, Integer> fn;
	private HashMap<Integer, Functions> fun;
	private ArrayList<Integer> aggrs;
	private ArrayList<ArrayList<LeafValue>> tupleList;
	private int index;
	@SuppressWarnings("rawtypes")
	private List elements;
	private ArrayList<Integer> indices;
	private ArrayList<SelectExpressionItem> alias = new ArrayList<SelectExpressionItem>();

	@SuppressWarnings("rawtypes")
	public Aggregation(Operator oper, List elements, ArrayList<SelectExpressionItem> alias, HashMap<Integer, Integer> fn) {
		this.oper = oper;
		this.elements = elements;
		if(alias != null) this.alias = new ArrayList<SelectExpressionItem>(alias);
		this.fn = fn;
		init();
	}
	
	// Initializes the HashMap by reading all the tuples at once.
	private void init(){
		map = new HashMap<String, ArrayList<LeafValue>>();
		indices = new ArrayList<Integer>();
		aggrs = new ArrayList<Integer>();
		tupleList = new ArrayList<ArrayList<LeafValue>>();
		alias = new ArrayList<SelectExpressionItem>();
		fun = new HashMap<Integer, Functions>();
		index = 0;
		if(elements != null){
			int index = -1;
			// Associates the alias to the real column name.
			for(Object obj : elements){
				String colName = ((Column) obj).getColumnName();
				String tableName = ((Column) obj).getTable().getName();
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
								index = i;
								break;	
							}
							else if(schema.get(i).getTable().getAlias().toLowerCase().equals(tableName.toLowerCase())){
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
				indices.add(index);
			}
		}
		for (Entry<Integer, Integer> entry : fn.entrySet()) {
			fun.put(entry.getKey(), new Functions(entry.getValue()));
		}
		ArrayList<LeafValue> tuple;
		
		// Read one tuple at a time and put it inside HashMap w/ Key as GROUP BY Clauses
		while((tuple=oper.readOneTuple()) != null){
			String key = getKey(tuple);
			ArrayList<LeafValue> result = map.get(key);
			if(result != null){
				long count = 1;
				try {
					count = result.get(result.size()-1).toLong() + 1;
					result.set(result.size()-1, new LongValue(count));
				} catch (InvalidLeaf e) {}
				for (Entry<Integer, Functions> entry : fun.entrySet()) {
					int index = entry.getKey();
					Functions aggr = entry.getValue();
					aggr.accept(result.get(index), tuple.get(index));
					result.set(index, aggr.receive());
				}
			}
			else{
				tuple.add(new LongValue(1));
				map.put(key, tuple);
			}
		}
		
		// Checks what kind of Aggregation operation to perform, ie, SUM|COUNT|MIN|MAX and assigns it an ID
		for (Entry<Integer, Functions> entry : fun.entrySet()) {
			if(entry.getValue().getType() == 2){
				aggrs.add(entry.getKey());
			}
		}
		
		// Performs the aggregation on the Values of the HashMap and returns a new Array List of Tuples
		for (Entry<String, ArrayList<LeafValue>> entry : map.entrySet()) {
			ArrayList<LeafValue> row = entry.getValue();
			long count = 1;
			try {
				count = row.get(row.size()-1).toLong() + 1;
			} catch(Exception e){}
			for(int aggrIndex : aggrs){
				LeafValue cell = row.get(aggrIndex);
				try {
					row.set(aggrIndex, new DoubleValue(cell.toDouble()/count));
				} catch (InvalidLeaf e) {}
			}
			row.remove(row.size()-1);
			tupleList.add(row);
		}
		map = new HashMap<String, ArrayList<LeafValue>>();
	}
	
	// Creates a key for HashMap based on the Values of GROUP BY clauses in each row of data
	public String getKey(ArrayList<LeafValue> tuple){
		String key = "";
		if(indices.size() > 0){
			for(int i=0; i<indices.size(); i++){
				key += tuple.get(indices.get(i)) + "|";
			}
		}
		else{
			key = "0";
		}
		key = key.substring(0, key.length()-1);
		if(key.length() < 10) return key;
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {}
		md.update(key.getBytes());
		byte byteData[] = md.digest();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
			sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		key = sb.toString().substring(0, 10);
		return key;
	}
	
	// Converts a tuple to String
	public String tupleToString(ArrayList<LeafValue> row){
		String tuple = "";
		for(LeafValue lValue : row){
			String str;
			if(lValue == null) str = "";
			else str = lValue.toString();
			if(lValue instanceof StringValue) tuple += str.substring(1, str.length()-1) + "|";
			else tuple += str + "|";
		}
		tuple = tuple.substring(0, tuple.length()-1);
		return tuple;
	}
	
	// Converts String to a Tuple
	public ArrayList<LeafValue> stringToTuple(String str){
		ArrayList<LeafValue> tuple = new ArrayList<LeafValue>();
		String[] array = str.split("\\|");
		for(int i=0; i<array.length; i++){
			tuple.add(getLeaf(array[i]));
		}
		return tuple;
	}
	
	// Function to identify the data type of the leaf value
	public LeafValue getLeaf(String str){
		if(isInteger(str) && !str.matches(".*[a-zA-Z]+.*")) return new LongValue(str);
		else if(charCounter(str, '-') == 2 && !str.matches(".*[a-zA-Z]+.*")) return new DateValue(" " + str + " ");
		else if(charCounter(str, '.') == 1 && !str.matches(".*[a-zA-Z]+.*")) return new DoubleValue(str);
		else return new StringValue(" " + str + " ");
	}
	
	public boolean isInteger(String s) {
	    return isInteger(s,30);
	}

	public boolean isInteger(String s, int radix) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),radix) < 0) return false;
	    }
	    return true;
	}
	
	public int charCounter(String str, char character){
		int counter = 0;
		for(int i=0; i<str.length(); i++) {
		    if(str.charAt(i) == character) {
		        counter++;
		    } 
		}
		return counter;
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