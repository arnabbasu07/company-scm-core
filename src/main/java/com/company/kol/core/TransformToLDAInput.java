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

	private static String[] rootWords = {"battlefield","hors","bunker","threat","storm","triumph","ga","knock","agit","traumat","pulver","reconnaiss",
		"relentless","unifi","jeer","jet","concuss","upris","clandestin","torch","enforc","grenad","death","conquer","toppl","neutral","despot","carnag",
		"aim","stealth","disast","deadli","loathsom","ammunit","betrai","apartheid","rival","helicopt","mayhem","downfal","wage","ricochet","guard","notori",
		"airfield","tragic","blindsid","militari","vanguard","retali","aggress","hazard","suffer","checkpoint","join","trampl","holocaust","oner","encount","sieg",
		"intern","engag","deploi","warhead","attrit","zone","bullet","warrant","ralli","quiver","nightmar","maraud","march","rocket","civilian","overthrow",
		"cataclysm","dead","tank","frenzi","vital","offens","watch","arsen","grave","regiment","resist","zigzag","sanction","persecut","victori","machin","dread",
		"pound","feroci","alli","militarist","powder","fanat","surpris","author","ruthless","concentr","pilot","lament","fuel","interven","dictat","quarrel",
		"terrain","straggler","slaughter","convoi","trauma","uniform","endur","ignit","chopper","disrupt","photo","confus","maim","feud","conspir","confront",
		"secur","acid","mission","govern","gener","spy","score","treacheri","repar","doom","sensor","bulletproof","pugnaci","zeal","carrier","terrorist",
		"assault","laser","torpedo","strategist","disastr","liber","grievou","screen","allianc","invas","support","rot","op","mobil","fugit","complianc",
		"careen","threaten","warrior","expung","flee","shoot","advanc","fortif","boobi","thwart","battl","interdict","agenc","rescu","courag","negoti","faction",
		"incontrovert","showdown","wound","hammer","petrifi","seizur","vilifi","dispers","turbul","whiz","collaps","partisan","vitriol","foxhol","evacue","void",
		"penetr","flight","garrison","plane","overrun","fearless","account","method","smuggl","hostil","chao","char","terror","survivor","defect","wisdom",
		"belliger","violat","refuge","disson","retreat","instruct","unbeliev","hit","revolut","train","captur","subvers","extrem","fatal","defiant","momentum",
		"violenc","groan","interrog","airplan","milit","aircraft","ferment","conflagr","duti","ambush","strateg","commando","activ","war","massacr","disarrai",
		"declin","regim","suspect","fright","ir","evacu","domin","potent","vocifer","dismantl","radiat","spokesman","intens","vile","howitz","guid","salvag",
		"nitrat","devic","furtiv","countermand","appeas","megalomania","disarma","warplan","fight","captiv","quail","rage","vulner","vanish","defens","diseas",
		"danger","recruit","veteran","hide","heroism","battalion","rifl","horrif","savag","involv","blood","demor","conflict","prei","posit","readi","ravish",
		"patrol","unconvent","intercept","run","strike","sabotag","exercis","hair","surrend","menac","barricad","missil","rift","debacl","preemptiv","insurrect",
		"guerrilla","front","kidnap","tourniquet","wreckag","dash","atroc","infiltr","charg","pistol","weapon","trench","keen","prowl","defend","coalit","suppress",
		"fear","drone","consequ","tactic","munit","round","perform","reinforc","harsh","platoon","genocid","malevol","clamor","rebel","control","watchdog","inform",
		"disciplin","heroic","sacrific","buri","militia","burn","scare","destroi","insurg","sedit","explos","corp","compass","opposit","automat","patriot","bloodlet",
		"breach","nationalist","ravag","dispatch","ashor","explod","strangl","mistreat","unleash","suppli","exploit","submarin","stronghold","unit","scrambl","alarm",
		"squad","conspiraci","investig","paramed","strife","tension","secreci","improvis","out","recoveri","shell","seiz","inflam","premedit","venom","vehicl","malici",
		"vendetta","spotter","drama","expect","airport","oper","mortar","surviv","consolid","devast","arm","enemi","setback","brutal","bombard","stash","pacifi","fieri",
		"superstit","alert","winc","fierc","warfar","viciou","soldier","skirmish","disput","barrag","incit","counterattack","yearn","cross","reaction","knive","deton",
		"link","launch","legaci","line","cargo","zealot","launcher","anarchi","transport","command","mine","campaign","reput","fighter","forc","annihil","trigger",
		"crisi","desert","hijack","post","struggl","artilleri","injuri","anguish","bloodi","radic","satellit","loyalti","demolish","gun","worldwid","vehicular","damag",
		"outbreak","prison","cadav","attack","retribut","intimid","muscl","quell","armament","hate","infer","intellig","armori","kill","shot","impact","urgenc","infantri",
		"duck","combat","power","offici","chief","provoc","hatr","culpabl","coordin","flank","murder","camouflag","strip","trap","gunship","yell","frai","bomb","hatch",
		"destruct","rebellion","execut","clash","warn","repris","virul","unexpect","blast","excess","incid","escal","escap","epithet","order","rule","die","xrai","debri",
		"corps","blow","cautionari","cautiou","plunder","vow","reveng","detect","watchlist","prolifer","aerial","storag","shock","aggressor","liaison","assassin","target","strategi","secret","special","casualti"
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
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/stemSecurityProfiles11","hdfs://hadoop-namenode:9000/user/dev11/securityLDAInput11","hdfs://hadoop-namenode:9000/user/dev11/securityDerivation11/"};
		transformToLDAInput(fpaths);
	}
}
