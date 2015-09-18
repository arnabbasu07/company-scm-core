package com.company.kol.core;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;
import com.twitter.Regex;

public class StopWordRemoval {

	final static String patternStr1 = "http://(linkd\\.in|t\\.co|bitly\\.co|tcrn\\.ch).*?(\\s|$)";//(linkd\\.in|t\\.co|bitly\\.co|tcrn\\.ch).*?(\s|$)";


	final String patternStr2 = "/(?:http?://)?(?:(?:0rz\\.tw)|(?:1link\\.in)|(?:1url\\.com)|(?:2\\.gp)|(?:2big\\.at)|(?:2tu\\.us)|(?:3\\.ly)|(?:307\\.to)|(?:4ms\\.me)|(?:4sq\\.com)|(?:4url\\.cc)|(?:6url\\.com)|(?:7\\.ly)|(?:a\\.gg)|(?:a\\.nf)|(?:aa\\.cx)"
			+ "|(?:abcurl\\.net)|(?:ad\\.vu)|(?:adf\\.ly)|(?:adjix\\.com)|(?:afx\\.cc)|(?:all\\.fuseurl.com)|(?:alturl\\.com)|(?:amzn\\.to)|(?:ar\\.gy)|(?:arst\\.ch)|(?:atu\\.ca)|(?:azc\\.cc)|(?:b23\\.ru)"
			+ "|(?:b2l\\.me)|(?:bacn\\.me)|(?:bcool\\.bz)|(?:binged\\.it)|(?:bit\\.ly)|(?:bizj\\.us)|(?:bloat\\.me)|(?:bravo\\.ly)|(?:bsa\\.ly)|(?:budurl\\.com)|(?:canurl\\.com)|(?:chilp\\.it)|(?:chzb\\.gr)|(?:cl\\.lk)|(?:cl\\.ly)|(?:clck\\.ru)|(?:cli\\.gs)|(?:cliccami\\.info)|(?:clickthru\\.ca)|(?:clop\\.in)|(?:conta\\.cc)|(?:cort\\.as)|(?:cot\\.ag)|(?:crks\\.me)|(?:ctvr\\.us)|(?:cutt\\.us)|(?:dai\\.ly)|(?:decenturl\\.com)|(?:dfl8\\.me)|(?:digbig\\.com)|(?:digg\\.com)|(?:disq\\.us)|(?:dld\\.bz)|(?:dlvr\\.it)|(?:do\\.my)|(?:doiop\\.com)|(?:dopen\\.us)|(?:easyuri\\.com)|(?:easyurl\\.net)|(?:eepurl\\.com)|(?:eweri\\.com)|(?:fa\\.by)|(?:fav\\.me)|(?:fb\\.me)|(?:fbshare\\.me)|(?:ff\\.im)|(?:fff\\.to)|(?:fire\\.to)|(?:firsturl\\.de)|(?:firsturl\\.net)|(?:flic\\.kr)|(?:flq\\.us)|(?:fly2\\.ws)|(?:fon\\.gs)|(?:freak\\.to)|(?:fuseurl\\.com)|(?:fuzzy\\.to)|(?:fwd4\\.me)|(?:fwib\\.net)|(?:g\\.ro.lt)|(?:gizmo\\.do)|(?:gl\\.am)|(?:go\\.9nl.com)|(?:go\\.ign.com)|(?:go\\.usa.gov)|(?:goo\\.gl)|(?:goshrink\\.com)|(?:gurl\\.es)|(?:hex\\.io)"
			/*+ "(?:hiderefer\\.com)|(?:hmm\\.ph)|(?:href\\.in)|(?:hsblinks\\.com)|(?:htxt\\.it)|(?:huff\\.to)|(?:hulu\\.com)|(?:hurl\\.me)|(?:hurl\\.ws)|(?:icanhaz\\.com)|(?:idek\\.net)|(?:ilix\\.in)|(?:is\\.gd)|(?:its\\.my)|(?:ix\\.lt)|(?:j\\.mp)|(?:jijr\\.com)|(?:kl\\.am)|(?:klck\\.me)|(?:korta\\.nu)|(?:krunchd\\.com)|(?:l9k\\.net)|(?:lat\\.ms)|(?:liip\\.to)|(?:liltext\\.com)|(?:linkbee\\.com)|(?:linkbun\\.ch)|(?:liurl\\.cn)|(?:ln-s\\.net)|(?:ln-s\\.ru)|(?:lnk\\.gd)|(?:lnk\\.ms)|(?:lnkd\\.in)|(?:lnkurl\.com)|(?:lru\.jp)|(?:lt\.tl)|(?:lurl\.no)|(?:macte\.ch)|(?:mash\.to)|(?:merky\.de)|(?:migre\.me)|(?:miniurl\.com)|(?:minurl\.fr)|(?:mke\.me)|(?:moby\.to)|(?:moourl\.com)|(?:mrte\.ch)|(?:myloc\.me)|(?:myurl\.in)|(?:n\.pr)|(?:nbc\.co)|(?:nblo\.gs)|(?:nn\.nf)|(?:not\.my)|(?:notlong\.com)|(?:nsfw\.in)|(?:nutshellurl\.com)|(?:nxy\.in)|(?:nyti\.ms)|(?:o-x\.fr)|(?:oc1\.us)|(?:om\.ly)|(?:omf\.gd)|(?:omoikane\.net)|(?:on\.cnn.com)|(?:on\.mktw.net)|(?:onforb\.es)|(?:orz\.se)|(?:ow\.ly)|(?:ping\.fm)|(?:pli\.gs)|(?:pnt\.me)|(?:politi\.co)|(?:post\.ly)|(?:pp\.gg)|(?:profile\.to)|"
			+ "(?:ptiturl\.com)|(?:pub\.vitrue.com)|(?:qlnk\.net)|(?:qte\.me)|(?:qu\.tc)|(?:qy\.fi)|(?:r\.im)|(?:rb6\.me)|(?:read\.bi)|(?:readthis\.ca)|(?:reallytinyurl\.com)|(?:redir\.ec)|(?:redirects\.ca)|(?:redirx\.com)|(?:retwt\.me)|(?:ri\.ms)|(?:rickroll\.it)|(?:riz\.gd)|(?:rt\.nu)|(?:ru\.ly)|(?:rubyurl\.com)|(?:rurl\.org)|(?:rww\.tw)|(?:s4c\.in)|(?:s7y\.us)|(?:safe\.mn)|(?:sameurl\.com)|(?:sdut\.us)|(?:shar\.es)|(?:shink\.de)|(?:shorl\.com)|(?:short\.ie)|(?:short\.to)|(?:shortlinks\.co.uk)|(?:shorturl\.com)|(?:shout\.to)|(?:show\.my)|(?:shrinkify\.com)|(?:shrinkr\.com)|(?:shrt\.fr)|(?:shrt\.st)|(?:shrten\.com)|(?:shrunkin\.com)|(?:simurl\.com)|(?:slate\.me)|(?:smallr\.com)|(?:smsh\.me)|(?:smurl\.name)|(?:sn\.im)|(?:snipr\.com)|(?:snipurl\.com)|(?:snurl\.com)|(?:sp2\.ro)|(?:spedr\.com)|(?:srnk\.net)|(?:srs\.li)|(?:starturl\.com)|(?:su\.pr)|(?:surl\.co.uk)|(?:surl\.hu)|(?:t\.cn)|(?:t\.co)|(?:t\.lh.com)|(?:ta\.gd)|(?:tbd\.ly)|(?:tcrn\.ch)|(?:tgr\.me)|(?:tgr\.ph)|(?:tighturl\.com)|(?:tiniuri\.com)|(?:tiny\.cc)|(?:tiny\.ly)|(?:tiny\.pl)|"
			+ "(?:tinylink\.in)|(?:tinyuri\.ca)|(?:tinyurl\.com)|(?:tl\.gd)|(?:tmi\.me)|(?:tnij\.org)|(?:tnw\.to)|(?:tny\.com)|(?:to\.ly)|(?:togoto\.us)|(?:totc\.us)|(?:toysr\.us)|(?:tpm\.ly)|(?:tr\.im)|(?:tra\.kz)|(?:trunc\.it)|(?:twhub\.com)|(?:twirl\.at)|(?:twitclicks\.com)|(?:twitterurl\.net)|(?:twitterurl\.org)|(?:twiturl\.de)|(?:twurl\.cc)|(?:twurl\.nl)|(?:u\.mavrev.com)|(?:u\.nu)|(?:u76\.org)|(?:ub0\.cc)|(?:ulu\.lu)|(?:updating\.me)|(?:ur1\.ca)|(?:url\.az)|(?:url\.co.uk)|(?:url\.ie)|(?:url360\.me)|(?:url4\.eu)|(?:urlborg\.com)|(?:urlbrief\.com)|(?:urlcover\.com)|(?:urlcut\.com)|(?:urlenco\.de)|(?:urli\.nl)|(?:urls\.im)|(?:urlshorteningservicefortwitter\.com)|(?:urlx\.ie)|(?:urlzen\.com)|(?:usat\.ly)|(?:use\.my)|(?:vb\.ly)|(?:vgn\.am)|(?:vl\.am)|(?:vm\.lc)|(?:w55\.de)|(?:wapo\.st)|(?:wapurl\.co.uk)|(?:wipi\.es)|(?:wp\.me)|(?:x\.vu)|(?:xr\.com)|(?:xrl\.in)|(?:xrl\.us)|(?:xurl\.es)|(?:xurl\.jp)|(?:y\.ahoo.it)|(?:yatuc\.com)|(?:ye\.pe)|(?:yep\.it)|(?:yfrog\.com)|(?:yhoo\.it)|(?:yiyd\.com)|(?:youtu\.be)|(?:yuarel\.com)|(?:z0p\.de)|(?:zi\.ma)|"
			+ "(?:zi\.mu)|(?:zipmyurl\.com)|(?:zud\.me)|(?:zurl\.ws)|(?:zz\.gd)|(?:zzang\.kr)|(?:›\.ws)|(?:✩\.ws)|(?:✿\.ws)|(?:❥\.ws)|(?:➔\.ws)|(?:➞\.ws)|(?:➡\.ws)|(?:➨\.ws)|(?:➯\.ws)|(?:➹\.ws)|(?:➽\.ws))\*/
			+")/[a-z0-9]\\*/";

	static{
		filesPath= new HadoopPaths();
	}
	static HadoopPaths filesPath;

	static FSDataOutputStream out;
	static StringBuffer fsb;
	static FileSystem fs;
	public static void removeStopWords(final String[] fpaths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local").
					setAppName(" Stop Word Removal "));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0]);

			JavaPairRDD<Long, String> IdTweetMap = lines.mapToPair(
					new PairFunction<String, Long, String>() {
						public Tuple2<Long, String> call(String s) throws Exception {
							//							System.out.println(" Initial String "+ s);
							if(s.indexOf("\t")==-1)
								return null;
							if(!NumberUtils.isNumber(s.substring(0, s.indexOf("\t"))))
									return null;
							Long id = Long.valueOf(s.substring(0, s.indexOf("\t")));
							String initialTweet = s.replace(String.valueOf(id), "").replaceAll("\\t", " ");
							System.out.println(" Initial String "+initialTweet);
							String removedUrl = checkAndRemoveURL(initialTweet);
							String removedHashTag = checkAndRemoveHashtag(removedUrl);
							String removedRetweet = checkAndRemoveRetweet(removedHashTag);
							String removedatRateUser = checkAndRemoveatRateUser(removedRetweet);
							String vanillaString = checkAndRemoveSpecialCharacters(removedatRateUser);
							return new Tuple2<Long, String>(id, vanillaString);		
						}
					}
					).filter(
							new Function<Tuple2<Long,String>, Boolean>() {

								public Boolean call(Tuple2<Long, String> v1)
										throws Exception {
									return v1 != null;
								}
							
							}
							);

			JavaRDD<String> slines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[1]);
			final String[] swords = (String[]) slines.collect().toArray(new String[(int)slines.count()]);
			String[] temp2 = new String[swords.length+1];
			String[] temp = new String[swords.length+1];
			for(int i=0; i< swords.length; i++){
				temp[i] = " ";
				temp2[i] = " "+swords[i]+" ";
			}
			temp[swords.length] = " ";
			temp2[swords.length] = " RT ";
			final String[] rwords = temp;
			final String[] iwords = temp2;

			JavaPairRDD<Long,Iterable<String>> idNewMap = IdTweetMap.mapToPair(
					new PairFunction<Tuple2<Long,String>, Long, String>() {
						public Tuple2<Long, String> call(Tuple2<Long, String> t) throws Exception {
							return new Tuple2<Long, String>(t._1,StringUtils.replaceEach(t._2, iwords, rwords));
						}
					}
					).groupByKey();
			/*map(
					new Function<String, String>() {

						public String call(String s) throws Exception {

							return StringUtils.replaceEach(s, iwords, rwords);
						}
					}
					);
			idTweetMap.foreach(
					VoidFunction<Tuple2<Long,String>>(){
						public void call(Tuple2<Long, String> t){
							System.out.println("   "+ t);
						}
					});*/
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			fs = FileSystem.get(conf);
			if(fs.exists(new Path(fpaths[2])))
				fs.delete(new Path(fpaths[2]));
			
			idNewMap.foreach(
					new VoidFunction<Tuple2<Long,Iterable<String>>>() {
						public void call(Tuple2<Long,Iterable<String>> t) throws IllegalArgumentException, IOException{
							out = fs.create(new Path(fpaths[2]+File.separator+t._1));
							fsb = new StringBuffer();
							Iterator<String> it = t._2.iterator();
							while(it.hasNext()){
								String s = it.next();
								fsb.append(s);
								fsb.append("\n");
							}
							out.writeBytes(fsb.toString());
							out.close();
						}
					}
					);

		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}

	protected static String checkAndRemoveSpecialCharacters(
			String s) {
		Pattern pattern = Pattern.compile("[^a-zA-Z0-9\\t\\s+]+",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed Special Characters  "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	/*protected static String removePattern(String s) {
		Pattern pattern = Pattern.compile(patternStr1);
		Matcher matcher = pattern.matcher(s);
		String k = null;
		if (matcher.find()) {
	         k = s.replaceAll(matcher.group(0), "");
	      } 
		return k;
	}

	private static boolean checkPattern(String s) {
		Pattern pattern = Pattern.compile(patternStr1);
		Matcher matcher = pattern.matcher(s);
		return matcher.find();
	}*/
	///\brt\s*@(\w+)/i
	private static String checkAndRemoveRetweet(String s){
		Pattern pattern = Pattern.compile("\\b\\s(RT|rt)\\s*@(\\w+|\\d+):\\s*\\b",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed Retweet  "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	private static String checkAndRemoveatRateUser(String s){
		Pattern pattern = Pattern.compile("\\b\\s*@(\\w+|\\d+|\\w+\\d+|\\w+_\\w+|\\w+_\\d+)");
		Matcher matcher = pattern.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed @user "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	private static String checkAndRemoveHashtag(String s){
		Matcher matcher =  Regex.VALID_HASHTAG.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed #tag "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	private static String checkAndRemoveURL(String s){
		Matcher matcher =  Regex.VALID_URL.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed url "+ matcher.replaceAll(""));
		return matcher.replaceAll("");
	}
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/userTweets.txt","hdfs://hadoop-namenode:9000/user/dev11/stopwords.txt","hdfs://hadoop-namenode:9000/user/dev11/profiles"};
		StopWordRemoval.removeStopWords(fpaths);
		/*StopWordRemoval stRemoval = new StopWordRemoval();
		System.out.println("   "+ stRemoval.checkAndRemoveRetweet("asda ad 123213 1 rt @asdsad: blah blah http://t.co   #a12324 avinash12313"));*/
	}

}
