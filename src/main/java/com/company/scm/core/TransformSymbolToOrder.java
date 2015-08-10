package com.company.scm.core;


import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;
import com.company.commons.core.MapUtil;

public class TransformSymbolToOrder {

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;

	static FSDataOutputStream out;
	static StringBuffer fsb;
	
	private static Map<Integer, Long> orderSymbolMap = new HashMap<Integer, Long>();
	
	private static Map<Long, Integer> symbolOrderMap = new HashMap<Long, Integer>();
	
	public static void transformAdjacentList(String[] fpaths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local").setAppName(" Transform to Graph Adjacent List"));
			
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0]);
			
			JavaRDD<String> lineRDD = lines.map(
					new Function<String, String>() {
						int count = 0;
						public String call(String s){
							String[] ids = StringUtils.split(s);
							for(String id:ids){
								if(symbolOrderMap.get(Long.valueOf(id))==null){
									symbolOrderMap.put(Long.parseLong(id), count);
									orderSymbolMap.put(count, Long.valueOf(id));
									count++;
								}
							}
							return s;
						}
					}
					);
			lineRDD.count();
			
			System.out.println(symbolOrderMap +"  total vertices "+symbolOrderMap.size());
			Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get( new URI( filesPath.HDFS_URL ),conf );
			Path fpath = new Path(fpaths[1]);
			if (fs.exists(fpath)) {
				fs.delete(fpath, true);			   
			}
			out = fs.create(fpath);
			fsb = new StringBuffer();
			fsb.append(symbolOrderMap.size());
			fsb.append("\n");
			fsb.append(lineRDD.count());
			fsb.append("\n");
			out.writeBytes(fsb.toString());
			out.flush();
			MapUtil.sortByValue(symbolOrderMap);
			lineRDD.foreach(
					new VoidFunction<String>() {
						
						public void call(String s) throws Exception {
							String[] words = StringUtils.split(s);
							fsb = new StringBuffer();
							StringBuffer sb = new StringBuffer();
							for(String word :words){
								sb.append(" ");
								sb.append(symbolOrderMap.get(Long.parseLong(word)));
							}
							fsb.append(sb.substring(1));
							fsb.append("\n");
							out.writeBytes(fsb.toString());
							out.flush();
						}
					});
			out.close();
			
			Path fpath_2 = new Path(fpaths[2]);
			if (fs.exists(fpath_2)) {
				fs.delete(fpath_2, true);			   
			}
			FSDataOutputStream out = fs.create(fpath_2);
			StringBuffer sb ; 
			for(Entry<Long, Integer> e:symbolOrderMap.entrySet()){
				sb = new StringBuffer();
				sb.append(e.getKey());
				sb.append(" ");
				sb.append(e.getValue());
				sb.append("\n");
				out.writeBytes(sb.toString());
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
		String[] fpaths = {"/home/hduser/work/friendslist_4.csv","/user/dev11/orderGraph_2.csv","/user/dev11/symbolOrderMap_2.csv"};
		transformAdjacentList(fpaths);
	}
}
