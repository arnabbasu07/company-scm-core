package com.company.scm.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.company.commons.core.HadoopPaths;
import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;

import facebook4j.Comment;
import facebook4j.Facebook;
import facebook4j.FacebookException;
import facebook4j.FacebookFactory;
import facebook4j.PagableList;
import facebook4j.Post;
import facebook4j.Reading;
import facebook4j.ResponseList;
import facebook4j.User;
import facebook4j.conf.Configuration;
import facebook4j.conf.ConfigurationBuilder;
import facebook4j.internal.org.json.JSONArray;
import facebook4j.internal.org.json.JSONException;
import facebook4j.internal.org.json.JSONObject;



public class FacebookUtil {

	private static Logger log = LoggerFactory.getLogger(FacebookUtil.class);

	static{
		filesPath= new HadoopPaths();
	}	
	static HadoopPaths filesPath;

	private static Facebook getFacebookInstance(){
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthAppId("234325323402662")
		.setOAuthAppSecret("0d7b58e8ebb505Fbb5931e20ab54aec3")
		.setOAuthAccessToken("234325323402662|FTEqveXyBMD7lAZVJMBdNLUxTA0")
//		FTEqveXyBMD7lAZVJMBdNLUxTA0
		.setOAuthPermissions("email, publish_stream, id, name, first_name, last_name, read_stream , generic")
		.setUseSSL(true)
		.setJSONStoreEnabled(true);

		Configuration configuration = cb.build(); 

		FacebookFactory factory = new FacebookFactory(configuration);
		return factory.getInstance();
	}

	private static FacebookClient getFBClient(){
		FacebookClient facebookClient = new DefaultFacebookClient("234325323402662|FTEqveXyBMD7lAZVJMBdNLUxTA0", "0d7b58e8ebb505Fbb5931e20ab54aec3");
		return facebookClient;
	}
	private static class FqlPost {
		//	    @Facebook("post_id")
		String post_id;

		//	    @Facebook("created_time")
		String created_time;

		@Override
		public String toString() {
			return String.format("%s,%s", post_id, created_time);
		}
	}
	
	public static void getUserPosts(List<String> ids){
		FacebookClient fbclient = FacebookUtil.getFBClient();
		for(String id:ids){
			String query ="select post_id from stream where "
					+ "source_id ="+ id +" limit 3000" ;
			List<FqlPost> fqlPosts = fbclient.executeFqlQuery(query, FqlPost.class);
			for (FqlPost pagePost : fqlPosts) {            
				System.out.println(pagePost);
			}
			Connection<Post> myFeed = fbclient.fetchConnection("\""+id+"/feed", Post.class);
			for(List<Post> myFeedConnectionPage : myFeed)
			  for (Post post : myFeedConnectionPage)
			    System.out.println("Post: " + post);
		}
	}
	public static void getUserPosts4j(List<String> ids){
		/*Facebook facebook = FacebookUtil.getFacebookInstance();
		for(String key:ids){
			try{
				boolean fileSave = false; 
				String home = System.getProperty("user.home");
				// write data into local file
				File file = new File(home+File.separator+"f_"+key.toLowerCase()+".txt");
				if(file.exists()){
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " already exists");
					file.delete();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " deleted");
				}
				if (!file.exists()) {
					file.createNewFile();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " created");
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				String query ="select post_id from stream where "
						+ "source_id ="+ key +" limit 3000" ;
				JSONArray jsonArray = facebook.executeFQL(query);
				System.out.println(" json Array "+jsonArray);
				for (int i = 0; i < jsonArray.length(); i++) {
					JSONObject jsonObject = jsonArray.getJSONObject(i);
					if(null != facebook.getPost((String) jsonObject.get("post_id")) && null != facebook.getPost((String) jsonObject.get("post_id")).getMessage()){
						writer.write(facebook.getPost((String) jsonObject.get("post_id")).getMessage()+"\n");						
						System.out.println(" Post Id "+jsonObject.get("post_id")+" message "+ facebook.getPost((String) jsonObject.get("post_id")).getMessage());						
					}
					if(null!= facebook.getPost((String) jsonObject.get("post_id")).getStory())
						writer.write(facebook.getPost((String) jsonObject.get("post_id")).getStory()+"\n");
					writer.flush();
				}				
				writer.close();
				fileSave = true;
				Thread.sleep(65000);
			}catch(JSONException e){
				e.printStackTrace();
			}
			Connection<Post> myFeed = fbclient.fetchConnection("\""+id+"/feed", Post.class);
			for(List<Post> myFeedConnectionPage : myFeed)
			  for (Post post : myFeedConnectionPage)
			    System.out.println("Post: " + post); catch (FacebookException e) {
			    	// TODO Auto-generated catch block
			    	e.printStackTrace();
			    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}*/
	}
	public static String getUserTimeline(List<String> ids){
		Facebook facebook = FacebookUtil.getFacebookInstance();		
		int max_posts = 10;
		for(String key :ids){
			boolean fileSave = false; 
			String home = System.getProperty("user.home");
			try {
				// write data into local file
				File file = new File(home+File.separator+"f_"+key.toLowerCase()+".txt");
				if(file.exists()){
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " already exists");
					file.delete();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " deleted");
				}
				if (!file.exists()) {
					file.createNewFile();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " created");
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));

				User user = facebook.getUser(key, new Reading().fields("email"));
				System.out.println(" user "+user.getUsername());
				ResponseList<Post> feeds = facebook.getPosts(key,
			            new Reading().limit(max_posts));

				// For all 25 feeds...
		        for (int i = 0; i < feeds.size(); i++) {
		            // Get post.
		            Post post = feeds.get(i);
		            // Get (string) message.
		            String message = post.getMessage();
		                            // Print out the message.
		            System.out.println(message);

		            // Get more stuff...
		            PagableList<Comment> comments = post.getComments();
		            String date = post.getCreatedTime().toString();
		            String name = post.getFrom().getName();
		            String id = post.getId();
		        } 

			}catch (FacebookException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{		
				if(fileSave)
					log.info(home+File.separator+key+" .txt saved sucessfully");
				else
					log.error("Failed to save "+ home+File.separator+key+".txt");
			}
		}
		return "hello";
	}

	public static String searchPosts(List<String> keywords){
		int max_posts = 10000;
		Facebook facebook = FacebookUtil.getFacebookInstance();
		for(String key :keywords){
			boolean fileSave = false; 
			String home = System.getProperty("user.home");
			try {
				// write data into local file
				File file = new File(home+File.separator+"f_"+key.toLowerCase()+".txt");
				if(file.exists()){
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " already exists");
					file.delete();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " deleted");
				}
				if (!file.exists()) {
					file.createNewFile();
					log.info("File "+home+File.separator+"f_"+key.toLowerCase()+".txt" + " created");
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));

				ResponseList<Post> results = facebook.getFeed(key,new Reading().limit(max_posts));
				for (Post post : results) { 
					writer.write(post.getMessage()+"\n"); 
				}
				writer.flush();
				writer.close();
				fileSave = true;
				Thread.sleep(65000);
			} catch (FacebookException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{		
				if(fileSave)
					log.info(home+File.separator+key+" .txt saved sucessfully");
				else
					log.error("Failed to save "+ home+File.separator+key+".txt");
			}	

		}
		FileSystem hdfs =null;
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		String home = System.getProperty("user.home");
		try{
			hdfs = FileSystem.get( new URI( filesPath.HDFS_URL ), conf);
			for(String key :keywords){
				Path localFilePath = new Path(home+File.separator+"f_"+key+".txt");
				Path modelPath = new Path("/user/dev11"+File.separator+"f_"+key+".txt");
				if (hdfs.exists(modelPath)) {
					hdfs.delete(new Path("/user/dev11"+File.separator+"f_"+key+".txt"), true);			   
				}
				hdfs.copyFromLocalFile(localFilePath, modelPath);
			}
		}catch(IOException e){
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		return "";
	}
	
	public static void main(String[] args) {
		String[] fpaths = {"file:///home/dev11/work/twitter_ids_full.csv","hdfs://localhost:9000/user/dev11/"};
		//		FacebookUtil.getUsersTimeLine(fpaths);
		List<String> keywords = new ArrayList<String>();
		keywords.add("vmware");
		keywords.add("VMworld");
		keywords.add("Virtualization");
		keywords.add("vcloud");
		keywords.add("vsphere");
		keywords.add("vspp");
		keywords.add("Virtual Hybrid Cloud");	
		FacebookUtil.searchPosts(keywords);
		List<String> ids = new ArrayList<String>(){{
			add("536347852");
			add("100001612975448");
			add("536347852");
			add("742169542");
			add("100006378255943");
			add("100006650850905");
			add("100000603046460");
			add("1263846508");
			add("100000518465669");
			add("698360720");
			add("162635247124349");
			add("1726561240");
			add("100000212922767");
			add("712060");
			add("1233901892");
			add("100001496198288");
			add("113184250589");
			add("665280091");
			add("527837396");
			add("1391863073");
			add("737515524");

			add("37900670");

			add("25823680");

			add("1544990061");

			add("100004018516599");

			add("100003207902250");
			add("1671177802");

			add("72404856");

			add("534626622");

			add("664642709");

			add("1767332788");

			add("1271436556");

			add("1176707432");

			add("100000078917258");

			add("100001278620424");

			add("567826769");

			add("722188985");

			add("636411986");

			add("1015854121");

			add("100000303500804");

			add("100001000508867");

			add("724980693");

			add("1429417505");

			add("1072713231");

			add("812910000");

			add("438909236183275");

			add("1139977685");

			add("508846904");

			add("670548478");
		}};
		FacebookUtil.getUserPosts4j(ids);
	}
}
