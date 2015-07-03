package com.company.scm.core;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.company.commons.core.CommonFunctions;

//import com.company.commons.core.CommonFunctions;

public class EstimateSocialDistance {

	public static Map<String, Double> estimateSocialDistance(String[] fpaths){
		JavaSparkContext ctx = null;
		Map<String, Double> map = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setAppName("Estimate Social Distance")

					.setMaster("local"));
			
			JavaRDD<String> elines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0]);
			
			JavaRDD<String> lines_1 = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[1]);
			
			JavaRDD<String> lines_2 = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[2]);
			
			JavaRDD<String> lines = lines_1.union(lines_2);
			
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return map;
	}
	public static void main(String[] args) {
		String[] fpaths = {"/home/hduser/work/Employees_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne_2.csv",""};
		estimateSocialDistance(fpaths);
	}
}
