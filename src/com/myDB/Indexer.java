package com.myDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/*

The indexer class that creates the environment for indexing the data and creates an index based on the key and the value.

*/

public class Indexer {
	
	private static Environment myDbEnvironment = null;
	private static Database myDatabase = null;
	
	public static void createPrimary(String fileName, int colNo){
		File file = new File(Metadata.dataDir + File.separator + fileName + ".dat");
		BufferedReader input = null;
		try{
			EnvironmentConfig envConfig = new EnvironmentConfig();
		    envConfig.setAllowCreate(true);
		    myDbEnvironment = new Environment(Metadata.indexDir, envConfig);
		    DatabaseConfig dbConfig = new DatabaseConfig();
		    dbConfig.setAllowCreate(true);
		    myDatabase = myDbEnvironment.openDatabase(null, fileName, dbConfig);
			input = new BufferedReader (new FileReader(file));
			String line = null;
			String key = "";
			String value = "";
			while((line=input.readLine()) != null){
				String[] array = line.split("\\|");
				key = array[colNo];
				value = line;
				DatabaseEntry dbKey = new DatabaseEntry(key.getBytes("UTF-8"));
			    DatabaseEntry dbValue = new DatabaseEntry(value.getBytes("UTF-8"));
			    if (myDatabase.get(null, dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			    	byte[] retData = dbValue.getData();
			    	String foundData = new String(retData, "UTF-8");
			    	value = foundData + "\n" + value;
			    	dbValue = new DatabaseEntry(value.getBytes("UTF-8"));
			    }
			    myDatabase.put(null, dbKey, dbValue);
			}
			if(myDatabase != null)  myDatabase.close();
			if(myDbEnvironment != null) myDbEnvironment.close();
		}
		catch (DatabaseException e) {
			System.out.println("DB Exception Occured.");
		}
		catch(IOException e){
			System.out.println("IO Exception Occured.");
			input = null;
		}
	}
}