package com.company.kol.core;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;
import com.company.scm.model.TwitterProfile;


public class TransformRawInput {

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;

	static FSDataOutputStream out;
	static FSDataOutputStream pout;
	static StringBuffer fsb;
	static StringBuffer pfsb;
	static Set<String> set = new HashSet<String>();
	public static void preprocess(String[] paths){
		try{

			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(paths[0]));
			JavaSparkContext ctx;
			JavaPairRDD<Long, String> finalIdCommentMap = null; 
			ctx = new JavaSparkContext(new SparkConf().setMaster("local")
					.setAppName("Preprocess Data for LDA"));
			if(fs.exists(new Path(paths[1])))
				fs.delete(new Path(paths[1]));
			out = fs.create(new Path(paths[1]));
			
			if(fs.exists(new Path(paths[2])))
				fs.delete(new Path(paths[2]), true);
			pout = fs.create(new Path(paths[2]));
			for(int i=0; i< status.length; i++){
				JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, status[i].getPath().toString());
				System.out.println(" File "+ StringUtils.splitPreserveAllTokens(status[i].getPath().toString(), "/")[StringUtils.splitPreserveAllTokens(status[i].getPath().toString(), "/").length-1]+" lines count "+lines.count());
				if(lines.count() == 0)
					continue;

				JavaPairRDD<Long,Iterable<String>> idCommentMap = lines.mapToPair(
						new PairFunction<String, Long, String>() {
							StringBuffer sb;
							Long id;
							public Tuple2<Long, String> call(String s)
									throws Exception {
								String[] words = StringUtils.splitPreserveAllTokens(s, "\t");
								if(words.length > 1 && NumberUtils.isNumber(words[0])){
									//									System.out.println(" with id "+s );
									if(null != sb && sb.toString().length() > 0){
										Tuple2<Long, String> t = new Tuple2<Long, String>(id, sb.toString());
										sb = new StringBuffer();
										sb.append(words[1]);
										sb.append("\t");
										if(words.length > 2)
											sb.append(words[2]+" ");
										id = new Long(Long.parseLong(words[0]));;
										//										System.out.println("   "+ t);
										return t;
									}
									id = new Long(Long.parseLong(words[0]));
									sb = new StringBuffer();
									sb.append(words[1]);
									sb.append("\t");
									if(words.length > 2)
										sb.append(words[2]+" ");
								}else{
									//									System.out.println(" without id "+s );
									if(null != sb)
										sb.append(s+" ");
								}
								return null;
							}

						}
						).filter(
								new Function<Tuple2<Long,String>, Boolean>() {

									public Boolean call(Tuple2<Long, String> v1) throws Exception {
										// TODO Auto-generated method stub
										return v1 != null;
									}
								}
								).groupByKey();/*.groupByKey().mapValues(
										new Function<Iterable<String>, Iterable<String>>() {

											public Iterable<String> call(
													Iterable<String> v1)
													throws Exception {
												Set<String> set = new HashSet<String>();
												Iterator<String> it = v1.iterator();
												while(it.hasNext()){
													String s = it.next();
													if(set.contains(s))
														continue;
													set.add(s);
												}
												System.out
														.println(" Set "+ set);
												return set;
											}

										}
										)*/;
										//				idCommentMap.count();
										/*map(
										new Function<Tuple2<Long,String>, String>() {
											public String call(
													Tuple2<Long, String> v1)
													throws Exception {
												StringBuffer sb = new StringBuffer();
												sb.append(v1._1);
												sb.append("\t");
												sb.append(v1._2);
												return sb.toString();
											}

										}
										).filter(
												new Function<String, Boolean>() {
													StringBuffer sb;
													public Boolean call(
															String v1)
															throws Exception {
														sb = new StringBuffer();
														return null;
													}

												}
												)*/;/*.mapValues(
										new Function<Iterable<String>, Iterable<String>>() {

											public Iterable<String> call(
													Iterable<String> v1)
													throws Exception {
												Set<String> set = new HashSet<String>();
												Iterator<String> it = v1.iterator();
												while(it.hasNext()){
													String s = it.next();
													if(set.contains(s))
														continue;
													set.add(s);
												}
												return set;
											}

										}
										);
												 */				/*idCommentMap.foreach(
						new VoidFunction<Tuple2<Long,String>>() {

							public void call(Tuple2<Long, String> t) throws Exception {
								Set<String> set = new HashSet<String>();
								Iterator<String> it = t._2.iterator();
								while(it.hasNext()){
									String s = it.next();
									if(set.contains(s))
										continue;
									set.add(s);
								}
								System.out.println(set.size()+"  ");
							}
						});*/
												//				idCommentMap.count();
												/**
												 * map the user with the comments and text
												 * 
												 */
												/*lines.foreach(
						new VoidFunction<String>() {

							public void call(String s) throws Exception {
								// TODO Auto-generated method stub
								System.out.println(" each line "+ s);
							}
						});*/
												/*if(null == finalIdCommentMap){
					finalIdCommentMap = idCommentMap;
					System.out.println(" Individual Count "+ idCommentMap.count() );
				}
				else{
					joinedMap = finalIdCommentMap.sortByKey(true).join(idCommentMap.sortByKey(true));
					System.out.println(" Individual Count "+ idCommentMap.count() +" Aggregate count  "+joinedMap.count());
				}*/

												idCommentMap.foreach(
														new VoidFunction<Tuple2<Long,Iterable<String>>>() {

															public void call(Tuple2<Long, Iterable<String>> t)
																	throws Exception {
																fsb = new StringBuffer();
																pfsb = new StringBuffer();
																Iterator<String> it = t._2.iterator();
//																Set<String> set = new HashSet<String>();
																while(it.hasNext()){
																	String s = it.next();
																	String[] words = StringUtils.splitPreserveAllTokens(s, "\t");
//																	if(!set.contains(words[1])){
																		fsb.append(t._1);
																		fsb.append("\t");
																		fsb.append(s);
																		fsb.append("\n\n");
//																		set.add(words[1]);
//																	}
																	if(!set.contains(words[0])){
																		pfsb.append(words[0]);
																		pfsb.append("\n");
																	}
																}
																out.writeBytes(fsb.toString());
																out.hsync();
																pout.writeBytes(pfsb.toString());
																pout.hsync();
															}

														}
														);								
			}
			out.close();
			pout.close();
			ctx.stop();

		} catch(Exception e){
			System.out.println("File not found");
			e.printStackTrace();
		}

	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/securityKeywords","hdfs://hadoop-namenode:9000/user/dev11/securityTweets.txt","hdfs://hadoop-namenode:9000/user/dev11/profiles.txt"};
		preprocess(fpaths);
	}
}
