package com.company.kol.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.company.commons.core.CommonFunctions;

public class TopicDerivationForUsers {

	private static String[] rootWords = {"advertise",
		"agent",
		"agency",
		"brand",
		"campaign",
		"sales",
		"survey",
		"contract",
		"design",
		"customer",
		"market",
		"crm",
		"segment",
		"profit",
		"loss",
		"product",
		"launch",
		"promotion",
		"positioning",
		"trade",
		"vendor",
		"media",
		"merchandise",
		"niche",
		"portfolio",
		"trend",
		"relation",
		"business",
		"event",
		"performance",
		"mail",
		"logo",
		"budget",
		"manage",
		"consult",
		"buy",
		"sell",
		"spectrum",
		"contact",
		"consumer"
		};
	static FSDataOutputStream out;
	public static void getDerivedTopics(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local")
					.setAppName(" Derive Topics"));
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(paths[0]));
			
			if(fs.exists(new Path(paths[1])))
				fs.delete(new Path(paths[1]));
			
			out = fs.create(new Path(paths[1]));
			
			for(int i=0; i< status.length; i++){
				String path = status[i].getPath().toString();
				JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, path);
				if(lines.count()==1){
					String line = lines.collect().get(0);
					List<String> words = Arrays.asList(StringUtils.split(line));
					List<Double> values = new ArrayList<Double>();
					for(String word:words)
						values.add(Double.valueOf(word));
					double value = Collections.max(values);
					int index = values.indexOf(value);
					out.writeBytes(StringUtils.splitPreserveAllTokens(path, "/")[StringUtils.splitPreserveAllTokens(path, "/").length-1]+"\t"+rootWords[index]+"\n");
					out.hsync();
				}else{
					List<String> list = lines.collect();
					List<Double> values = new ArrayList<Double>();
					for(int k=0; k< list.size(); k++){
						String[] words = StringUtils.split(list.get(k));
						for(int j=0; j< words.length; j++){
							if(k==0){
								values.add(Double.valueOf(words[j]));
							}else
								values.set(j, values.get(j)+ Double.valueOf(words[j]));
						}
					}
					double value = Collections.max(values);
					int index = values.indexOf(value);
					out.writeBytes(StringUtils.splitPreserveAllTokens(path, "/")[StringUtils.splitPreserveAllTokens(path, "/").length-1]+"\t"+rootWords[index]+"\n");
					out.hsync();
				}
			}
			out.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/Derivation/","hdfs://hadoop-namenode:9000/user/dev11/derivedOutput.txt"};
		getDerivedTopics(fpaths);
	}
}
