package com.company.kol.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	public static void modelTopics(String[] paths){
		JavaSparkContext ctx = null;
		try{
			ctx = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("LDA Modelling"));
			
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(paths[0]));
			
			/*if(fs.exists(new Path(paths[1])))
				fs.delete(new Path(paths[1]));*/
			
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
			    
			 // Cluster the documents into three topics using LDA
			    DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(3).run(corpus);
			    
			 // Output topics. Each is a distribution over words (matching word count vectors)
			    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
			        + " words):");
			    Matrix topics = ldaModel.topicsMatrix();
			    for (int topic = 0; topic < 3; topic++) {
			      System.out.print("Topic " + topic + ":");
			      for (int word = 0; word < ldaModel.vocabSize(); word++) {
			        System.out.print(" " + topics.apply(word, topic));
			      }
			      System.out.println();
			    }
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null != ctx)
				ctx.stop();
		}
	}
	
	public static void main(String[] args) {
		String[] fpaths = {"hdfs://hadoop-namenode:9000/user/dev11/LDAInput",""};
		modelTopics(fpaths);
	}
}
