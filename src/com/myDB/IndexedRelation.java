package com.myDB;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

// Class that uses the data from the indexer to fetch indexed data.

public class IndexedRelation{

	private String tableName;
	private ArrayList<Column> schema;
	private int[] array = new int[20];
	private Environment myDbEnvironment = null;
	private Database myDatabase = null;
	private Expression expr;
	private ConditionVisitor eval;
	
	
	public IndexedRelation(String tableName, Expression expr){
		this.tableName = tableName;
		this.expr = expr;
		@SuppressWarnings({ "rawtypes"})
		List objs = Metadata.schemas.get(tableName.toUpperCase()).getColumnDefinitions();
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
	
	
	// If the key found in the index, return all matched data.
	public ArrayList<ArrayList<LeafValue>> getBatch(String key){
		ArrayList<ArrayList<LeafValue>> batch = null;
		try{
		    DatabaseEntry dbKey = new DatabaseEntry(key.getBytes("UTF-8"));
		    DatabaseEntry dbData = new DatabaseEntry();
		    if (myDatabase.get(null, dbKey, dbData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    	batch = new ArrayList<ArrayList<LeafValue>>();
		    	byte[] retData = dbData.getData();
		    	String foundData = new String(retData, "UTF-8");
			    String[] lines = foundData.split("\n");
			    for(String line : lines){
					String[] cols = line.split("\\|");
					ArrayList<LeafValue> ret = new ArrayList<LeafValue>();
					Integer[] list = Code.getRelationMap(tableName);
					for (int i = 0; i<list.length; i++){
						ret.add(getLeaf(cols[list[i]], array[list[i]]));
					}
					eval.setTuple(ret);
					try {
						if(((BooleanValue) eval.eval(expr)).getValue()){
							batch.add(ret);
						}
					} catch (SQLException e) {}
			    }
			    if(batch.size() == 0) batch = null;
		    }
			return batch;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("A database exception has occured.");
			return batch;
		}
	}
	
	public LeafValue getLeaf(String str, int type){
		switch(type){
		case 0: return new LongValue(str);
		case 1 : return new DoubleValue(str);
		case 2 : return new StringValue(" " + str + " ");
		case 3 : return new DateValue(" " + str + " ");
		default : return new LongValue(str);
		}
	}
	
	public void reset(){
		schema = new ArrayList<Column>();
		@SuppressWarnings("rawtypes")
		List objs = Metadata.schemas.get(tableName.toUpperCase()).getColumnDefinitions();
		Integer[] list = Code.getRelationMap(tableName);
		Table table = new Table();
		table.setName(tableName);
		for (int i = 0; i<list.length; i++){
			schema.add(new Column(table, ((ColumnDefinition) (objs.get(list[i]))).getColumnName()));
		}
		EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    myDbEnvironment = new Environment(Metadata.indexDir, envConfig);
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    myDatabase = myDbEnvironment.openDatabase(null, tableName.toUpperCase(), dbConfig);
	    eval = new ConditionVisitor(getSchema(), null);
	}

	public ArrayList<Column> getSchema() {
		return schema;
	}
}