package com.myDB;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/*

Class to sort the tuples using External SortedMap

*/

class ExternalSort implements Operator{
	private Operator oper;
	private LinkedList<ArrayList<LeafValue>> tupleList;
	private int index;
	@SuppressWarnings("rawtypes")
	private List elements;
	private ArrayList<Integer> indices;
	private ArrayList<Boolean> bools;
	private ArrayList<SelectExpressionItem> alias = new ArrayList<SelectExpressionItem>();
	private int MAX_ROWS = 2;
	private int fileNo = 1;
	private int mergeIter = 1;
	private boolean isRec = false;
	private ArrayList<Integer> dataTypes = new ArrayList<Integer>();
	
	@SuppressWarnings("rawtypes")
	public ExternalSort(Operator oper, List elements, ArrayList<SelectExpressionItem> alias) {
		this.oper = oper;
		this.elements = elements;
		if(alias != null) this.alias = new ArrayList<SelectExpressionItem>(alias);
		init();
	}
	
	private void init(){
		tupleList = new LinkedList<ArrayList<LeafValue>>();
		indices = new ArrayList<Integer>();
		bools = new ArrayList<Boolean>();
		index = 0;
		
		// Initialize the schema.
		for(Object obj : elements){
			bools.add(((OrderByElement) obj).isAsc());
			String[] array = ((OrderByElement) obj).getExpression().toString().split("\\.");
			String colName = null;
			String tableName = null;
			if(array.length == 2){
				colName = array[1];
				tableName = array[0];
			}
			else{
				colName = array[0];
			}
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
							indices.add(i);
							break;	
						}
						else if(schema.get(i).getTable().getAlias().toLowerCase().equals(tableName.toLowerCase())){
							indices.add(i);
							break;	
						}
					}
					else{
						indices.add(i);
						break;
					}
				}
			}
		}
		fileNo = 1;
		int count = 0;
		ArrayList<LeafValue> tuple;
		
		while((tuple=oper.readOneTuple()) != null){
			if(isRec == false){
				isRec = true;
				recDataType(tuple);
			}
			count++;
			tupleList.add(tuple);
			if(count == MAX_ROWS){ // Read till maximum limit reached.
				int prevIndex = -1;
				for(int i=0; i<indices.size(); i++){
					Algorithm sort = new Algorithm();
					sort.sort(tupleList, indices.get(i), prevIndex, bools.get(i), 0); // Sort the data.
					prevIndex = indices.get(i);
				}
				count = 0;
				writeToDisk(fileNo); // Write to disk.
				fileNo++;
				tupleList = new LinkedList<ArrayList<LeafValue>>();
			}
		}
		if(tuple == null && count != 0){
			int prevIndex = -1;
			for(int i=0; i<indices.size(); i++){
				Algorithm sort = new Algorithm();
				sort.sort(tupleList, indices.get(i), prevIndex, bools.get(i), 0);
				prevIndex = indices.get(i);
			}
			count = 0;
			writeToDisk(fileNo);
			fileNo++;
			tupleList = new LinkedList<ArrayList<LeafValue>>();
		}
		
		// Merge all sorted files.
		mergeFiles();
	}
	
	// Merge all sorted files. (NEEDS TO BE PATCHED)
	private void mergeFiles(){
		int mergeFileNo = 1;
		for(int i=1; i<=fileNo; i++){
			File file1 = new File(Metadata.swapDir.getName() + File.separator + "FILE_" + mergeIter + "_" + i);
			i++;
			if(i <= fileNo){
				//File file2 = new File(Metadata.swapDir.getName() + File.separator + "FILE_" + mergeIter + "_" + i+1);
			}
			else{
				file1.renameTo(new File(Metadata.swapDir.getName() + File.separator + "FILE_" + (mergeIter+1) + "_" + mergeFileNo));
			}
			mergeFileNo++;
		}
		if(fileNo % 2 == 0) fileNo = fileNo / 2;
		else fileNo = (fileNo / 2) + 1;
		if(fileNo != 1){
			mergeIter++;
			mergeFiles();
		}
	}
	
	private void writeToDisk(int fileNo){
		File file = new File(Metadata.swapDir.getName() + File.separator + "FILE_1_" + fileNo);
		String data = "";
		for(ArrayList<LeafValue> tuple : tupleList){
			data += tupleToString(tuple) + "\n";
		}
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			writer.write(data);
			writer.close();
		} catch (IOException e) {}
	}

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
	
	private void recDataType(ArrayList<LeafValue> tuple){
		for(LeafValue lf : tuple){
			if(lf instanceof LongValue) dataTypes.add(1);
			else if(lf instanceof DoubleValue) dataTypes.add(2);
			else if(lf instanceof StringValue) dataTypes.add(3);
			else if(lf instanceof DateValue) dataTypes.add(4);
			else dataTypes.add(1);
		}
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