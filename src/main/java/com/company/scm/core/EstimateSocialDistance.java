package com.company.scm.core;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;
import com.company.scm.model.Employee;
import com.company.scm.model.TwitterProfile;

//import com.company.commons.core.CommonFunctions;

public class EstimateSocialDistance {

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;
	
	static FSDataOutputStream out;
	static StringBuffer fsb;
	
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
			
			final double count = lines.count();
			
			JavaRDD<String> DDlines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[3]);
			
			final Map<Integer, String> colINameMap = CommonFunctions.getColINameMap(DDlines); 
			
			JavaPairRDD<Integer, Double> missValuePairRDD = lines.flatMapToPair(
					new PairFlatMapFunction<String, Integer, Double>() {

						public Iterable<Tuple2<Integer, Double>> call(
								String s) throws Exception {
							String[] words = StringUtils.splitPreserveAllTokens(s.substring(0,s.indexOf('[')-1), ",");
							List<Tuple2<Integer, Double>> list = new ArrayList<Tuple2<Integer, Double>>();
							for(int i=0; i< words.length; i++){
								if(null == words[i] || words[i].equalsIgnoreCase("null") || words[i].isEmpty())
									list.add(new Tuple2<Integer, Double>(i, 1.0));
							}
							if(s.indexOf("[")-s.indexOf("]")< 2){
								list.add(new Tuple2<Integer, Double>(words.length, 1.0));
							}
							return list;
						}
					}).reduceByKey(
							new Function2<Double, Double, Double>() {
								
								public Double call(Double d1, Double d2) throws Exception {
									return d1+d2;
								}
							}).mapValues(
									new Function<Double, Double>() {

										public Double call(Double d)
												throws Exception {
											return d/count;
										}
										
									}
									); 
			map = new HashMap<String, Double>();
			for(Tuple2<Integer, Double> t:missValuePairRDD.collect()){
				if(null != colINameMap.get(t._1))
					map.put(colINameMap.get(t._1), (t._2*100));
			}
			
			JavaPairRDD<Integer, Iterable<TwitterProfile>> eIdTProfilePairRDD = 
					lines.mapToPair(
							new PairFunction<String, Integer, TwitterProfile>() {

								public Tuple2<Integer, TwitterProfile> call(
										String s) throws Exception {
									final String[] words = StringUtils.splitPreserveAllTokens(s.substring(0,s.indexOf('[')-1), ",");
									if(words.length != colINameMap.size()-1)
										return null;
									TwitterProfile twProfile = new TwitterProfile();
									twProfile.setEmployeeId(NumberUtils.isNumber(words[0])?Integer.parseInt(words[0]):0);
									twProfile.setName(words[1]);
									twProfile.setDepartment(words[2]);
									twProfile.setGender(words[3]);
									twProfile.setDesignation(words[4]);
									twProfile.setLocation(words[5]);
									twProfile.setEmailId(words[6]);
									twProfile.setLanguages(
											new ArrayList<String>(){{
												add(words[7].equalsIgnoreCase("en")?"English":words[7]);
											}});
									twProfile.setTimezone(words[8]);
									twProfile.setFriendlist(Arrays.asList(StringUtils.splitPreserveAllTokens(s.substring(s.indexOf("["), s.indexOf("]")),",")));
									return new Tuple2<Integer, TwitterProfile>(Integer.parseInt(words[0]), twProfile);
								}
								
							}
							).filter(
									new Function<Tuple2<Integer,TwitterProfile>, Boolean>() {

										public Boolean call(
												Tuple2<Integer, TwitterProfile> arg0)
												throws Exception {
											// TODO Auto-generated method stub
											return arg0!=null;
										}
										
									}).groupByKey();
			JavaPairRDD<Integer, Employee> eIdEProfilePairRDD =
					elines.mapToPair(
							new PairFunction<String, Integer, Employee>() {

								public Tuple2<Integer, Employee> call(
										String s) throws Exception {
									final String[] words = StringUtils.splitPreserveAllTokens(s, ",");
									Employee employee = new Employee();
									employee.setName(CommonFunctions.getFullName(
											(words[1].isEmpty()||words[1].equalsIgnoreCase("null"))?null:words[1], (words[2].isEmpty()||words[2].equalsIgnoreCase("null"))?null:words[2], (words[3].isEmpty()||words[3].equalsIgnoreCase("null"))?null:words[3]));
									employee.setDepartment(words[4].equalsIgnoreCase("null")?"":words[4]);
									employee.setGender(words[5].equalsIgnoreCase("null")?"":words[5]);
									employee.setDesignation(words[6].equalsIgnoreCase("null")?"":words[6]);
									employee.setLocation(words[7].equalsIgnoreCase("null")?"":words[7]);
									employee.setEmailId(words[8].equalsIgnoreCase("null")?"":words[8]);
									employee.setLanguages(
											new ArrayList<String>(){
												private static final long serialVersionUID = 1L;

											{
												for(int i=9; i< words.length; i++){
													add(words[i].replace("\"", ""));
												}
											}});
									return new Tuple2<Integer, Employee>(Integer.parseInt(words[0]), employee);
								}
							}
							);
			JavaPairRDD<Integer, Tuple2<Employee, Iterable<TwitterProfile>>> joinedPairRDD = eIdEProfilePairRDD.sortByKey(true).join(eIdTProfilePairRDD.sortByKey(true));
			
			Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get( new URI( filesPath.HDFS_URL ),conf );
			Path fpath = new Path(fpaths[4]);
			if (fs.exists(fpath)) {
				fs.delete(fpath, true);			   
			}
			out = fs.create(fpath);
			
			joinedPairRDD.foreach(
					new VoidFunction<Tuple2<Integer,Tuple2<Employee,Iterable<TwitterProfile>>>>() {
						
						public void call(
								Tuple2<Integer, Tuple2<Employee, Iterable<TwitterProfile>>> t)
								throws Exception {
							List<String> elist = t._2._1.getFriendlist();
							fsb = new StringBuffer();
							fsb.append(",");
							fsb.append(t._1);
							
							
						}
					});
			System.out.println(joinedPairRDD.count());
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return map;
	}
	
	public static double nameMatch(String s1, String s2){
		
		return 1.0;
	}
	public static void main(String[] args) {
		String[] fpaths = {"/home/hduser/work/Employees_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne_2.csv","/home/hduser/work/DataDefinition.csv","/user/dev11/output"};
		estimateSocialDistance(fpaths);
	}
}
