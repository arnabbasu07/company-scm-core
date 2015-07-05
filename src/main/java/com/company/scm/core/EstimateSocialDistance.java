package com.company.scm.core;

import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevensteinDistance;
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
import com.company.scm.model.TimeZone;
import com.company.scm.model.TwitterProfile;

//import com.company.commons.core.CommonFunctions;

public class EstimateSocialDistance {

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;

	static FSDataOutputStream out;
	static StringBuffer fsb;

	private static DecimalFormat df = new DecimalFormat("##.###");
	
	public static Map<String, Double> estimateSocialDistance(String[] fpaths, final double cutoff){
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
			final Map<Integer, Double> missValMap = new HashMap<Integer, Double>();
			for(Tuple2<Integer, Double> t:missValuePairRDD.collect()){
				if(null != colINameMap.get(t._1))
					map.put(colINameMap.get(t._1), (t._2*100));
				missValMap.put(t._1, (t._2*100));
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
									twProfile.setTwitterId(NumberUtils.isNumber(StringUtils.splitPreserveAllTokens(s, ",")[StringUtils.splitPreserveAllTokens(s, ",").length-1])?Long.parseLong(StringUtils.splitPreserveAllTokens(s, ",")[StringUtils.splitPreserveAllTokens(s, ",").length-1]):0);
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
									employee.setEmployeeId(Integer.parseInt(words[0]));
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

			final Map<Integer, String> eIdEProfileMap = new HashMap<Integer, String>();
			for(Tuple2<Integer, Employee> t:eIdEProfilePairRDD.collect()){
				eIdEProfileMap.put(t._1, t._2.getName());
			}
			final Map<Integer, ArrayList<String>> eIdEFriendlist = new HashMap<Integer, ArrayList<String>>();
			for(final Integer eId :eIdEProfileMap.keySet()){
				eIdEFriendlist.put(eId, new ArrayList<String>(){{
					addAll(eIdEProfileMap.values());
					remove(eIdEProfileMap.get(eId));
				}});
			}
			System.out.println("Employe Friend list "+ eIdEFriendlist);
			JavaPairRDD<Integer, Tuple2<Employee, Iterable<TwitterProfile>>> joinedPairRDD = eIdEProfilePairRDD.sortByKey(true).join(eIdTProfilePairRDD.sortByKey(true));

			Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get( new URI( filesPath.HDFS_URL ),conf );
			Path fpath = new Path(fpaths[4]);
			if (fs.exists(fpath)) {
				fs.delete(fpath, true);			   
			}
			out = fs.create(fpath);
			System.out.println(joinedPairRDD.count());
			joinedPairRDD.foreach(
					new VoidFunction<Tuple2<Integer,Tuple2<Employee,Iterable<TwitterProfile>>>>() {

						public void call(
								Tuple2<Integer, Tuple2<Employee, Iterable<TwitterProfile>>> t)
										throws Exception {
							List<String> elist = eIdEFriendlist.get(t._2._1.getEmployeeId());
							fsb = new StringBuffer();
							fsb.append(t._1);
							Iterator<TwitterProfile> it = t._2._2.iterator();
							while(it.hasNext()){
								TwitterProfile twProfile = it.next();
								fsb.append(",");
								fsb.append(twProfile.getTwitterId());
								fsb.append(",");
								fsb.append(df.format(nameMatch(t._2._1.getName(), twProfile.getName())));
								fsb.append(",");
								fsb.append((t._2._1.getDepartment().isEmpty()||twProfile.getDepartment().isEmpty())?0:
									df.format(Math.max(new LevensteinDistance().getDistance(t._2._1.getDepartment(), twProfile.getDepartment()),new JaroWinklerDistance().getDistance(t._2._1.getDepartment(), twProfile.getDepartment()))));
								fsb.append(",");
								fsb.append((t._2._1.getGender().isEmpty()||twProfile.getGender().isEmpty())?0:
									df.format(Math.max(new LevensteinDistance().getDistance(t._2._1.getGender(), twProfile.getGender()),new JaroWinklerDistance().getDistance(t._2._1.getGender(), twProfile.getGender()))));
								fsb.append(",");
								fsb.append((t._2._1.getDesignation().isEmpty()||twProfile.getDesignation().isEmpty())?0:
									df.format(Math.max(new LevensteinDistance().getDistance(t._2._1.getDesignation(), twProfile.getDesignation()),new JaroWinklerDistance().getDistance(t._2._1.getDesignation(), twProfile.getDesignation()))));
								fsb.append(",");
								fsb.append((t._2._1.getLocation().isEmpty()||twProfile.getLocation().isEmpty())?0:
									df.format(Math.max(new LevensteinDistance().getDistance(t._2._1.getLocation(), twProfile.getLocation()), new JaroWinklerDistance().getDistance(t._2._1.getLocation(), twProfile.getLocation()))));
								fsb.append(",");
								fsb.append((t._2._1.getEmailId().isEmpty()||twProfile.getEmailId().isEmpty())?0:
									df.format(Math.max(new LevensteinDistance().getDistance(t._2._1.getEmailId(), twProfile.getEmailId()),new JaroWinklerDistance().getDistance(t._2._1.getEmailId(), twProfile.getEmailId()))));
								fsb.append(",");
								fsb.append(CollectionUtils.intersection(t._2._1.getLanguages(), twProfile.getLanguages()).size()/Math.max(t._2._1.getLanguages().size(),twProfile.getLanguages().size()));
								fsb.append(",");
								double tscore=0;
								for(String e :elist){
									double pmatch =0;
									for(String f:twProfile.getFriendlist()){
										if(nameMatch(e, f)>pmatch)
											pmatch = nameMatch(e, f);
									}
									tscore += pmatch;
								}
								fsb.append(df.format(tscore/elist.size()));
								fsb.append("\n");
								if(it.hasNext())
									fsb.append(t._1);
							}
							out.writeBytes(fsb.toString());
							out.flush();
						}
					});
			out.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return map;
	}

	public static double nameMatch(String s1, String s2){
		double matches =0;	
		if(s1.isEmpty() || s2.isEmpty())
			return 0;
		if(s1.equalsIgnoreCase(s2))
			return 1;
		String[] array1 = StringUtils.split(s1);
		String[] array2 = StringUtils.split(s2);
		for(int i=0; i< array1.length; i++){
			if(Arrays.asList(array2).contains(array1[i]))
				matches+=1;
			else{
				double pmatch=0, maxStr =0;
				for(int j =0; j< array2.length; j++){
					double tmatch = LCSubStr(array1[i].toCharArray(),array2[j].toCharArray(),array1[i].toCharArray().length,array2[j].toCharArray().length);
					if(tmatch >pmatch && ((array1[i].length()==1||array2[j].length()==1)?true:tmatch >2)){
						pmatch = LCSubStr(array1[i].toCharArray(),array2[j].toCharArray(),array1[i].toCharArray().length,array2[j].toCharArray().length);
						maxStr = Math.max(array1[i].length(), array2[j].length());
					}
				}
				matches +=maxStr==0?0:pmatch/maxStr;
			}
		}
		if(array1.length > array2.length)
			return matches/array1.length;
		else
			return matches/array2.length;
	}
	public static double LCSubStr(char[] X, char[] Y, int m, int n)
	{
	    // Create a table to store lengths of longest common suffixes of
	    // substrings.   Notethat LCSuff[i][j] contains length of longest
	    // common suffix of X[0..i-1] and Y[0..j-1]. The first row and
	    // first column entries have no logical meaning, they are used only
	    // for simplicity of program
	    double[][] LCSuff = new double[m+1][n+1];
	    double result = 0;  // To store length of the longest common substring
	 
	    /* Following steps build LCSuff[m+1][n+1] in bottom up fashion. */
	    for (int i=0; i<=m; i++)
	    {
	        for (int j=0; j<=n; j++)
	        {
	            if (i == 0 || j == 0)
	                LCSuff[i][j] = 0;
	 
	            else if (X[i-1] == Y[j-1])
	            {
	                LCSuff[i][j] = LCSuff[i-1][j-1] + 1;
	                result = Math.max(result, LCSuff[i][j]);
	            }
	            else LCSuff[i][j] = 0;
	        }
	    }
	    return result;
	}
	
	public static void main(String[] args) {
		String[] fpaths = {"/home/hduser/work/Employees_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne.csv","/home/hduser/work/TwitterProfiles_cafyne_2.csv","/home/hduser/work/DataDefinition.csv","/user/dev11/output"};
		estimateSocialDistance(fpaths,30.0);
//		System.out.println(EstimateSocialDistance.nameMatch("Jan Vosecky", "Jack Vondracek"));
	}
}
