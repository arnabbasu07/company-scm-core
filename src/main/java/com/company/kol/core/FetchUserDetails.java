package com.company.kol.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.company.commons.core.CommonFunctions;

public class FetchUserDetails {

	public static void getTwitterUserDetails(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf()
							.setAppName(" Fetch Twitter User Details").setMaster("local"));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, paths[0]);
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {};
		getTwitterUserDetails(fpaths);
	}
}
