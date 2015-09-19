package com.company.kol.core;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;

import com.company.commons.core.CommonFunctions;
import com.company.scm.utils.SocialNetworkUtils;

public class FetchUserDetails {

	static FSDataOutputStream out;
	static StringBuffer fsb;

	public static void getTwitterUserDetails(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf()
			.setAppName(" Fetch Twitter User Details").setMaster("local"));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, paths[0]);
			twitter4j.conf.Configuration conf = SocialNetworkUtils.createTwitterConfiguration();
			TwitterFactory twitterFactory = new TwitterFactory(conf);
			final Twitter twitter = twitterFactory.getInstance();
			Configuration fsconf = new Configuration();
			fsconf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			fsconf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(fsconf);
			if(fs.exists(new Path(paths[1])))
				fs.delete(new Path(paths[1]), true);
			out = fs.create(new Path(paths[1]));
			lines.foreach(
					new VoidFunction<String>() {

						public void call(String arg0) throws Exception {
							String[] words = StringUtils.splitPreserveAllTokens(arg0, "\t");
							User user = twitter.showUser(Long.valueOf(words[0]));
							fsb = new StringBuffer();
							fsb.append(words[0]);
							fsb.append(",");
							if(null != user.getName()){
								fsb.append(user.getName());
							}
							fsb.append(",");
							if(null != user.getScreenName()){
								fsb.append(user.getScreenName());
							}
							fsb.append(",");
							if(null != user.getProfileImageURL()){
								fsb.append(user.getProfileImageURL());
							}
							fsb.append(",");
							if(null != user.getLocation()){
								fsb.append(user.getLocation());
							}
							fsb.append("\t");
							fsb.append(words[1]);
							fsb.append("\n");
							out.writeBytes(fsb.toString());
							out.hsync();
							Thread.sleep(60000l);
						}
					});
			out.close();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/LDAOutput.txt","hdfs://hadoop-namenode:9000/user/dev11/FinalOutput.txt"};
		getTwitterUserDetails(fpaths);
	}
}
