package com.company.kol.core;

import java.io.File;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;
import com.company.scm.utils.TweetTextUtil;

public class TransformToSeparateLines {

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;

	static FSDataOutputStream out;
	static StringBuffer fsb;
	static Random random = new Random();;
	static FileSystem fs;
	public static void transformTolines(final String[] fpaths) {
		JavaSparkContext ctx = null;
		try{

			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			fs = FileSystem.get(conf);

			ctx = new JavaSparkContext(new SparkConf().setAppName("Transform Saperate Lines")
					.setMaster("local"));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0])
					.map(new Function<String, String>() {

						public String call(String s) throws Exception {
							return s.substring(s.indexOf("\t")+1);
						}

					});
			if(fs.exists(new Path(fpaths[1])))
				fs.delete(new Path(fpaths[1]));

			JavaRDD<String> flines = lines.flatMap(new FlatMapFunction<String, String>() {

				public Iterable<String> call(String s) throws Exception {
					BreakIterator breakIterator = BreakIterator.getSentenceInstance(Locale.US);
					breakIterator.setText(s);
					int start=breakIterator.first();
					int end=-1;
					List<String> sentences=new ArrayList<String>();
					while ((end=breakIterator.next()) != BreakIterator.DONE) {
						String sentence=s.substring(start,end);
						start=end;
						sentences.add(sentence);
					}
					return sentences;
				}
			});
			
			JavaPairRDD<Integer, Iterable<String>> pairRDD = flines.mapToPair(
					new PairFunction<String, Integer, String>() {

						public Tuple2<Integer, String> call(String s)
								throws Exception {
//							System.out.println(new Tuple2<Integer, String>(random.nextInt(8000), s));
							return new Tuple2<Integer, String>(random.nextInt(10), s);
						}
					}
					).groupByKey();
			
			pairRDD.foreach(
					new VoidFunction<Tuple2<Integer,Iterable<String>>>() {

						public void call(Tuple2<Integer, Iterable<String>> t)
								throws Exception {
							out = fs.create(new Path(fpaths[1]+ File.separator+t._1));
							fsb = new StringBuffer();
							Iterator<String> it = t._2.iterator();
							while(it.hasNext()){
								String s = it.next();
								String removedUrl = TweetTextUtil.checkAndRemoveURL(s);
								String removedHashTag = TweetTextUtil.checkAndRemoveHashtag(removedUrl);
								String removedRetweet = TweetTextUtil.checkAndRemoveRetweet(removedHashTag);
								String removedatRateUser = TweetTextUtil.checkAndRemoveatRateUser(removedRetweet);
								String vanillaString = TweetTextUtil.checkAndRemoveSpecialCharacters(removedatRateUser);
								String fstring = vanillaString.replaceAll("\\P{InBasic_Latin}", "");
								System.out.println(" final String "+fstring);
								if(!fstring.isEmpty()){
									fsb.append(fstring);
									fsb.append("\n");
								}
							}
							out.writeBytes(fsb.toString());
							out.close();
						}
					});
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/securityTweets11.txt","hdfs://hadoop-namenode:9000/user/dev11/securityTweetsFolder"};
		transformTolines(fpaths);
	}

}
