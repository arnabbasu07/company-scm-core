package com.company.scm.utils;

import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.company.commons.core.CommonFunctions;

public class FindMaxValueNode {

	public static void getMaximumNode(String[] fpaths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext( new SparkConf().setMaster("local").
					setAppName("find maximum node value"));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0]);
			JavaRDD<Double> values = lines.map(
					new Function<String, Double>() {

						public Double call(String s) throws Exception {
								// TODO Auto-generated method stub
							String[] words= StringUtils.split(s);
							if(words.length==2)
								return Double.valueOf(words[1]);
							return 0.0;
						}
					});
			System.out.println(" maximum value "+ Collections.max(values.collect()));
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {"/home/dev11/work/security_symbol.csv"};
		getMaximumNode(fpaths);
	}
}