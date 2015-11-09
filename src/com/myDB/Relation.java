package com.myDB;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/*

The most basic operator class that reads data from the file and gererates tuples from each line of data.

*/

public class Relation implements Operator{

	private BufferedReader input;
	private File file;
	private Metadata mData;
	private Table table;
	private ArrayList<Column> schema;
	private int[] array = new int[20];
	
	@SuppressWarnings("static-access")
	public Relation(Table table, Metadata mData){ // Read from file line by line and generate a tuple.
		this.file = new File(mData.dataDir + File.separator + table.getName().toUpperCase() + ".dat");
		this.mData = mData;
		this.table = table;
		@SuppressWarnings({ "rawtypes"})
		List objs = mData.schemas.get(table.getName().toUpperCase()).getColumnDefinitions();
		int count = 0;
		for(Object obj : objs){
			String type = ((ColumnDefinition) obj).getColDataType().toString().trim().toLowerCase().split(" ")[0];
			switch(type){
			case "int" : array[count] = 0; break;
			case "decimal" : array[count] = 1; break;
			case "char" : array[count] = 2; break;
			case "varchar" : array[count] = 2; break;
			case "string" : array[count] = 2; break;
			case "date" : array[count] = 3; break;
			}
			count++;
		}
		reset();
	}
	
	@Override
	public ArrayList<LeafValue> readOneTuple(){ // Return one tuple at a time
		if (input == null) {
			return null;
		}
		String line = null;
		try{
			line = input.readLine();
		}
		catch(IOException e){
			System.out.println("IO Exception Occured.");
		}
		if (line == null || line.equals("")) {
			return null;
		}
		String[] cols = line.split("\\|");
		ArrayList<LeafValue> ret = new ArrayList<LeafValue>();
		Integer[] list = Code.getRelationMap(table.getName());
		for (int i = 0; i<list.length; i++){
			ret.add(getLeaf(cols[list[i]], array[list[i]]));
		}
		return ret;
	}
	
	public LeafValue getLeaf(String str, int type){ // Convert the string to its corresponding datatype.
		switch(type){
		case 0: return new LongValue(str);
		case 1 : return new DoubleValue(str);
		case 2 : return new StringValue(" " + str + " ");
		case 3 : return new DateValue(" " + str + " ");
		default : return new LongValue(str);
		}
	}
	
	@Override
	public void dump(){
		Output.dump(this);
	}
	
	@SuppressWarnings("static-access")
	@Override
	public void reset(){ // Reset the schema and the buffer.
		schema = new ArrayList<Column>();
		@SuppressWarnings("rawtypes")
		List objs = mData.schemas.get(table.getName().toUpperCase()).getColumnDefinitions();
		Integer[] list = Code.getRelationMap(table.getName());
		for (int i = 0; i<list.length; i++){
			schema.add(new Column(table, ((ColumnDefinition) (objs.get(list[i]))).getColumnName()));
		}
		try{
			input = new BufferedReader (new FileReader(file));
		}
		catch(IOException e){
			System.out.println("IO Exception Occured.");
			input = null;
		}
	}

	@Override
	public ArrayList<Column> getSchema() {
		return schema;
	}
}