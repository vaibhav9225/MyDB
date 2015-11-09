package com.myDB;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

/*

The entry point for the Program
Takes a list of arguments including the input filenames for parsing and output filenames for indexing

*/

public class Main{
	private static ArrayList<File> sqlFiles = new ArrayList<File>();
	private static Metadata mData = new Metadata();

	@SuppressWarnings("static-access")
	public static void main(String args[]){
		boolean isLoadPhase = false;
		if(args.length == 0){
			mData.mode = "a";
			mData.indexDir = new File("db");
			mData.swapDir = new File("swap");
			mData.dataDir = new File("test" + File.separator + "data");
			sqlFiles.add(new File("test" + File.separator + "tables.sql"));
			if(isLoadPhase == false) sqlFiles.add(new File("test" + File.separator + "queries.sql"));
		}
		else{
			for(int i=0;i<args.length;i++){
				if(args[i].equals("--data")){ // Data directory
					mData.dataDir = new File(args[i+1]);
					i++;
				}
				else if(args[i].equals("--swap")){ // Swap Directory 
					mData.swapDir = new File(args[i+1]);
					i++;
				}
				else if(args[i].equals("--db")){ // Index Directory
					mData.indexDir = new File(args[i+1]);
					i++;
				}
				else if(args[i].equals("--debug")){
					mData.mode = args[i+1]; // a or e
					i++;
				}
				else if(args[i].equals("--load")){ // Load Queries
					isLoadPhase = true;
				}
				else {
					sqlFiles.add(new File(args[i])); // Input file names
				}
			}
		}
		long start = System.currentTimeMillis();
		// Create Index for Orders
		if(isLoadPhase){
			Indexer.createPrimary("ORDERS", 1);
			System.out.println("Index Created.");
		}
		
		// Iterate over each Query Statements
		for (File sql : sqlFiles) {
			try{
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				while((stmt = parser.Statement()) != null){
					
					
					if(stmt instanceof CreateTable){ // If create statement, store schema information
						CreateTable table = (CreateTable) stmt;
						mData.debugAll("Creating table: " + table.getTable().getName());
						mData.schemas.put(table.getTable().getName(), table);
					}
					else if(stmt instanceof Select){ // Else perform the query
						Code.accept(stmt.toString());
						StatementManager stmtManager = new StatementManager((Select) stmt, mData);
						Operator oper = stmtManager.select();
						mData.debugAll(oper.getSchema().toString());
						oper.dump();
						stmtManager.close();
					}
					else{
						System.out.println("Invalid Query Statement");
					}
				}
			}
			catch(IOException e){
				System.out.println("An IO Exception Occured.");
			}
			catch(ParseException e){
				System.out.println("A Parse Exception Occured.");
			}
		}
		long end = System.currentTimeMillis();
		if(mData.mode.equals("a") || mData.mode.equals("e")) System.out.println("\nExecution time: " + (end-start) + " milliseconds.");
	}
}