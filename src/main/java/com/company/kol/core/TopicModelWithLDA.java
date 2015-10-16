package com.company.kol.core;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

import com.company.commons.core.CommonFunctions;

public class TopicModelWithLDA {

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
		"corps","blow","cautionari","cautiou","plunder","vow","reveng","detect","watchlist","prolifer","aerial","storag","shock","aggressor","liaison","assassin","target",
		"strategi","secret","special","casualti"
	};
	static FSDataOutputStream out;
	
	public static void modelTopics(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("LDA Modelling"));

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
				JavaRDD<String> data = CommonFunctions.getJavaRDDFromFile(ctx, path);
				JavaRDD<Vector> parsedData = data.map(
						new Function<String, Vector>() {
							public Vector call(String s) {
								String[] sarray = s.trim().split(" ");
								double[] values = new double[sarray.length];
								for (int i = 0; i < sarray.length; i++)
									values[i] = Double.parseDouble(sarray[i]);
								return Vectors.dense(values);
							}
						}
						);

				// Index documents with unique IDs
				JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
						new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
							public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
								return doc_id.swap();
							}
						}
						));
				corpus.cache();


				double topic1 =0;int index1=0;
				double topic2 =0;int index2=0;
				double topic3 =0;int index3=0;
				// Cluster the documents into three topics using LDA
				for(int j=0; j< 10; j++){
					DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(3).run(corpus);
					// Output topics. Each is a distribution over words (matching word count vectors)
					System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
							+ " words):");
					List<Double> values1 = new ArrayList<Double>();
					List<Double> values2 = new ArrayList<Double>();
					List<Double> values3 = new ArrayList<Double>();
					Matrix topics = ldaModel.topicsMatrix();
					for (int topic = 0; topic < 3; topic++) {
						System.out.print("Topic " + topic + ":");
						for (int word = 0; word < ldaModel.vocabSize(); word++) {
							double value = topics.apply(word, topic);
							if(topic == 0)
								values1.add(value);
							if(topic == 1)
								values2.add(value);
							if(topic == 2)
								values3.add(value);
							System.out.print(" " + value);
						}
						System.out.println();
						if(topic==0 && Collections.max(values1)> topic1){
							topic1 = Collections.max(values1);
							index1 = values1.indexOf(topic1);
						}
						if(topic==1 && Collections.max(values2)> topic2){
							topic2 = Collections.max(values2);
							index2 = values2.indexOf(topic2);
						}
						if(topic==2 && Collections.max(values3)> topic3){
							topic3 = Collections.max(values3);
							index3 = values3.indexOf(topic3);
						}
					}
				}
				out.writeBytes(StringUtils.splitPreserveAllTokens(path, "/")[StringUtils.splitPreserveAllTokens(path, "/").length-1]+"\t"+rootWords[index1]+","+rootWords[index2]+","+rootWords[index3]+"\n");
				out.hsync();
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
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/securityLDAInput11","hdfs://hadoop-namenode:9000/user/dev11/securityLDAOutput11.txt"};
		modelTopics(fpaths);
	}
}
