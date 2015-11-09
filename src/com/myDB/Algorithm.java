package com.myDB;

import java.util.ArrayList;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;

/* 

Class that implements the sorting algorithm for ORDER BY

*/ 

public class Algorithm {
	private int number;
	private int index;
	private boolean isAsc;
	private int alg;
	private LinkedList<ArrayList<LeafValue>> tupleList = new LinkedList<ArrayList<LeafValue>>();
	private LinkedList<ArrayList<LeafValue>> tupleHelper = new LinkedList<ArrayList<LeafValue>>();

	public void sort(LinkedList<ArrayList<LeafValue>> tupleList, int index, int prevIndex, boolean isAsc, int alg) {
		this.tupleList = tupleList; // Input data.
		this.index = index; // Index of the column to sort by.
		this.isAsc = isAsc; // Order to sort by. ASC|DESC.
		this.alg = alg;
		this.number = tupleList.size();
	   	for (int i = 0; i <= number; i++) {
	   		tupleHelper.add(null);
	   	}
		if(prevIndex == -1) struct(0, number-1);
		else{
			if(number > 0 && prevIndex<tupleList.get(0).size()){
				LeafValue prev = tupleList.get(0).get(prevIndex);
				int pointer = 0;
				for(int i=0; i<tupleList.size(); i++){				
					if(i == tupleList.size()-1){
						if(equalTo(prev, tupleList.get(i).get(prevIndex))){
							struct(pointer, i);
						}
						else{
							struct(pointer, i-1);
							struct(i, i);
						}
					}
					else if(equalTo(prev, tupleList.get(i).get(prevIndex))) continue;
					else{
						struct(pointer, i-1);
						pointer = i;
						prev = tupleList.get(i).get(prevIndex);
					}
				}
			}
		}
	}
	
	// A function that limits the number of rows that needs to be sorted.
	private void struct(int start, int end){
		if(alg == 0) mergeSort(start, end);
		else quickSort(start, end);
	}
	
	private boolean eval(LeafValue lf1, LeafValue lf2, String str){
		if(isAsc == false){
			switch(str){
			case "<" : return compareGreaterThan(lf1, lf2);
			case ">" : return compareLessThan(lf1, lf2);
			case "<=" : return compareGreaterThanEqual(lf1, lf2);
			default : return compareLessThanEqual(lf1, lf2);
			}
		}
		else{
			switch(str){
			case ">" : return compareGreaterThan(lf1, lf2);
			case "<" : return compareLessThan(lf1, lf2);
			case ">=" : return compareGreaterThanEqual(lf1, lf2);
			default : return compareLessThanEqual(lf1, lf2);
			}
		}
	}
	
	// ---- A QUICK SORT IMPLEMENTATION FOR IN PLACE SORTING OF TUPLES ----
	
	private void quickSort(int lowerIndex, int higherIndex) {	
		int i = lowerIndex;
		int j = higherIndex;
		if((lowerIndex+higherIndex)/2 != 0){
			LeafValue pivot = tupleList.get((lowerIndex+higherIndex)/2).get(index);
			while(i <= j) {
				while(eval(tupleList.get(i).get(index),pivot, "<")) i++;
				while(eval(tupleList.get(j).get(index),pivot, ">")) j--;
				if(i <= j){
					swap(i, j);
					i++;
					j--;
				}
			}
			if(lowerIndex < j-1) quickSort(lowerIndex, j-1);
			if(i < higherIndex) quickSort(i, higherIndex);
		}
	}
	
    private void swap(int i, int j) {
    	if(!equalTo(tupleList.get(i).get(index), tupleList.get(j).get(index))){
	    	ArrayList<LeafValue> temp = tupleList.get(i);
	    	tupleList.set(i,tupleList.get(j));
	    	tupleList.set(j, temp);
    	}
    }
	
	// ---- A MERGE SORT IMPLEMENTATION (NOT USED) ----

	private void mergeSort(int low, int high) {
		if (low < high) {
			int middle = low + (high - low) / 2;
			mergeSort(low, middle);
			mergeSort(middle + 1, high);
			merge(low, middle, high);
		}
	}

	private void merge(int low, int middle, int high) {
	   	for (int i = low; i <= high; i++) {
	    	tupleHelper.set(i, tupleList.get(i));
	   	}
	   	int i = low;
	   	int j = middle + 1;
	   	int k = low;
	   	while (i <= middle && j <= high) {
	   		if (eval(tupleHelper.get(i).get(index),tupleHelper.get(j).get(index), "<=")) {
	   			tupleList.set(k, tupleHelper.get(i));
	   			i++;
	   		}
	   		else {
	   			tupleList.set(k, tupleHelper.get(j));
	   			j++;
	   		}
	   		k++;
	   	}
	   	while (i <= middle) {
	   		tupleList.set(k, tupleHelper.get(i));
	   		k++;
	   		i++;
	   	}
	  }
	
	private boolean equalTo(LeafValue lf1, LeafValue lf2){
		try{
			if(lf1 instanceof LongValue) return lf1.toLong()==lf2.toLong();
			else if(lf1 instanceof DoubleValue) return lf1.toDouble()==lf2.toDouble();
			else if(lf1 instanceof StringValue) return (lf1.toString().compareTo(lf2.toString())==0);
			else if(lf1 instanceof DateValue) return (lf1.toString().compareTo(lf2.toString())==0);
			else return false;
		}
		catch (InvalidLeaf e) {
			System.out.println("Invalid Leaf.");
			return false;
		}
	}

	private boolean compareGreaterThan(LeafValue lf1, LeafValue lf2){
		try{
			if(lf1 instanceof LongValue) return lf1.toLong()>lf2.toLong();
			else if(lf1 instanceof DoubleValue) return lf1.toDouble()>lf2.toDouble();
			else if(lf1 instanceof StringValue) return (lf1.toString().compareTo(lf2.toString())>0);
			else if(lf1 instanceof DateValue) return (lf1.toString().compareTo(lf2.toString())>0);
			else return false;
		}
		catch (InvalidLeaf e) {
			System.out.println("Invalid Leaf.");
			return false;
		}
	}
	
	private boolean compareLessThan(LeafValue lf1, LeafValue lf2){
		try{
			if(lf1 instanceof LongValue) return lf1.toLong()<lf2.toLong();
			else if(lf1 instanceof DoubleValue) return lf1.toDouble()<lf2.toDouble();
			else if(lf1 instanceof StringValue) return (lf1.toString().compareTo(lf2.toString())<0);
			else if(lf1 instanceof DateValue) return (lf1.toString().compareTo(lf2.toString())<0);
			else return false;
		}
		catch (InvalidLeaf e) {
			System.out.println("Invalid Leaf.");
			return false;
		}
	}
	
	private boolean compareGreaterThanEqual(LeafValue lf1, LeafValue lf2){
		try{
			if(lf1 instanceof LongValue) return lf1.toLong()>=lf2.toLong();
			else if(lf1 instanceof DoubleValue) return lf1.toDouble()>=lf2.toDouble();
			else if(lf1 instanceof StringValue) return (lf1.toString().compareTo(lf2.toString())>=0);
			else if(lf1 instanceof DateValue) return (lf1.toString().compareTo(lf2.toString())>=0);
			else return false;
		}
		catch (InvalidLeaf e) {
			System.out.println("Invalid Leaf.");
			return false;
		}
	}
	
	private boolean compareLessThanEqual(LeafValue lf1, LeafValue lf2){
		try{
			if(lf1 instanceof LongValue) return lf1.toLong()<=lf2.toLong();
			else if(lf1 instanceof DoubleValue) return lf1.toDouble()<=lf2.toDouble();
			else if(lf1 instanceof StringValue) return (lf1.toString().compareTo(lf2.toString())<=0);
			else if(lf1 instanceof DateValue) return (lf1.toString().compareTo(lf2.toString())<=0);
			else return false;
		}
		catch (InvalidLeaf e) {
			System.out.println("Invalid Leaf.");
			return false;
		}
	}
}