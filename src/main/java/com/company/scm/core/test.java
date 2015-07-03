package com.company.scm.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class test {

	public static void main(String[] args) {
		 SparkConf conf = new SparkConf (). setMaster ( "local" ). setAppName ( "My App" );
		 JavaSparkContext sc = new JavaSparkContext ( conf );
	}

}
