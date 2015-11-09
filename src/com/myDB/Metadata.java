package com.myDB;

import java.io.File;
import java.util.HashMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;

// A static Class that makes Metadata like filenames visible across all classes.

@SuppressWarnings("static-access")
public class Metadata {
	public static File dataDir = null;
	public static File swapDir = null;
	public static File queryDir = null;
	public static File indexDir = null;
	public static String mode = "";
	public static HashMap<String,CreateTable> schemas = new HashMap<String,CreateTable>();
	public static Column[] columns = null;

	public void debugAll(String str){
		if(this.mode.equals("e") || this.mode.equals("a")) System.out.println(str);
	}
	
	public void debugFew(String str){
		if(this.mode.equals("e")) System.out.println(str);
	}
}