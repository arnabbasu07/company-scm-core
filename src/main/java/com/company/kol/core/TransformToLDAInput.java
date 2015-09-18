package com.company.kol.core;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.company.commons.core.CommonFunctions;


public class TransformToLDAInput {

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
	static StringBuffer fsb;
	
	public static void transformToLDAInput(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local").setAppName(" Transform to LDA Input"));
			
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(paths[0]));
			
			if(fs.exists(new Path(paths[1])))
				fs.delete(new Path(paths[1]));
			
			if(fs.exists(new Path(paths[2])))
				fs.delete(new Path(paths[2]));
			/*Map<Integer, String> tempMap = new HashMap<Integer, String>();
			Map<String,Integer> tempMap2 = new HashMap<String, Integer>();
			for(int i=0; i< rootWords.length; i++){
				tempMap.put(i+1, rootWords[i]);
				tempMap2.put(rootWords[i], i+1);
			}
			final Map<Integer, String> indexRootWordMap = tempMap;
			final Map<String, Integer> rootIndexWordMap = tempMap2;*/
			for(int i=0; i< status.length; i++){
				String path = status[i].getPath().toString();
				JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, path)
						.filter(
						new Function<String, Boolean>() {
							
							public Boolean call(String v1) throws Exception {
								return (null != v1 && !v1.isEmpty());
							}
						}
						);
				if(lines.count() == 0)
					continue;
				
				JavaRDD<String> ilines = lines.map(
						new Function<String, String>() {

							public String call(String s) throws Exception {
								String[] words = StringUtils.split(s);
								StringBuffer sb = new StringBuffer();
								for(String root : rootWords){
									int count = 0;
									for(String word:words){
										if(word.startsWith(root))
											count++;
									}
									sb.append(count);
									sb.append(" ");
								}
								return sb.toString();
							}
							
						}
						);
				if(ilines.count() < 10 )
					out = fs.create(new Path(paths[2]+File.separator+StringUtils.splitPreserveAllTokens(path, "/")[StringUtils.splitPreserveAllTokens(path, "/").length-1]));
				else
					out = fs.create(new Path(paths[1]+File.separator+StringUtils.splitPreserveAllTokens(path, "/")[StringUtils.splitPreserveAllTokens(path, "/").length-1]));
				fsb = new StringBuffer();
				ilines.foreach(
						new VoidFunction<String>() {
							
							public void call(String s) throws Exception {
								System.out.println(" String Input "+ s);
								fsb.append(s);
								fsb.append("\n");
							}
						});
				out.writeBytes(fsb.toString());
				out.close();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/profiles","hdfs://hadoop-namenode:9000/user/dev11/LDAInput","hdfs://hadoop-namenode:9000/user/dev11/Derivation/"};
		transformToLDAInput(fpaths);
	}
}
