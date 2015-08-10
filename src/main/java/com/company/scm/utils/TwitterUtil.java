package com.company.scm.utils;

import java.io.BufferedWriter; 
import java.io.File; 
import java.io.FileWriter; 
import java.io.IOException; 
import java.net.URI; 
import java.net.URISyntaxException; 
import java.text.SimpleDateFormat; 
import java.util.ArrayList; 
import java.util.Calendar; 
import java.util.List;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.spark.SparkConf; 
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;

import twitter4j.Paging; 
import twitter4j.Query; 
import twitter4j.QueryResult; 
import twitter4j.Status; 
import twitter4j.Twitter; 
import twitter4j.TwitterException; 
import twitter4j.TwitterFactory; 
import twitter4j.conf.ConfigurationBuilder;

public class TwitterUtil {
	static{ 
		filesPath= new HadoopPaths(); 
	} 
	static HadoopPaths filesPath;
	private static Logger log = LoggerFactory.getLogger(TwitterUtil.class);

	private static Twitter getTwitterInstance(){ 
		ConfigurationBuilder cb = new ConfigurationBuilder(); 
		cb.setDebugEnabled(true) .setOAuthConsumerKey("lbDN1fZ9NkzalakdAY6cnNoiO") .
		setOAuthConsumerSecret("3ptghLg1VSaaKXXkRX6eg1z2iveibqdlADpHilj5mXNIacqD1p") .setOAuthAccessToken("2765400811-qCnaAk9r5VOMvWa3r55MagAB45jOA22BmpPi0Ht") .setOAuthAccessTokenSecret("iqBwC6HTgQxy9bb5HtZiLeQ8hQiaHFZ3G8L6SQeJKFhIq");
		TwitterFactory tfactory = new TwitterFactory(cb.build()); 
		return tfactory.getInstance(); 
	}

	public static void searchTweets(List<String> keywords,String date){ 
		int max_tweets = 3000; 
		twitter4j.conf.Configuration conf = SocialNetworkUtils.createTwitterConfiguration();//getTwitterInstance(); 
		TwitterFactory twitterFactory = new TwitterFactory(conf);
		Twitter twitter = twitterFactory.getInstance();
		for(String key :keywords){ 
			boolean fileSave = false; 
			String home = System.getProperty("user.home"); 
			try { 
				// write data into local file 
				File file = new File(home+File.separator+key.toLowerCase()+".txt"); 
				if(file.exists()){ 
					log.info("File "+home+File.separator+key.toLowerCase()+".txt" + " already exists"); 
					file.delete(); 
					log.info("File "+home+File.separator+key.toLowerCase()+".txt" + " deleted"); 
				} 
				if (!file.exists()) { 
					file.createNewFile(); 
					log.info("File "+home+File.separator+key.toLowerCase()+".txt" + " created"); 
				} 
				BufferedWriter writer = new BufferedWriter(new FileWriter(file)); 
				Query query = new Query(key.toLowerCase()); 
				query.setLang("en"); 
				query.setSince(date); 
				QueryResult result; 
				int count = 0; 
				do { 
					result = twitter.search(query); 
					List<Status> tweets = result.getTweets(); 
					for (Status tweet : tweets) { 
						writer.write(tweet.getUser().getId()+"\t"+tweet.getText()+"\n");
					} 
					count++; 
					writer.flush(); 
					Thread.sleep(65000l); 
				} while ((query = result.nextQuery()) != null && count < max_tweets); 
				writer.close(); 
				fileSave = true; 
			} catch (TwitterException te) { 
				te.printStackTrace(); 
				log.info("Failed to search tweets: " + te.getMessage()); 
				System.exit(-1); 
			}catch (IOException e) { 
				fileSave = false; 
				e.printStackTrace(); 
			} catch (InterruptedException e) { 
				// TODO Auto-generated catch block 
				e.printStackTrace(); 
			} finally{ 
				if(fileSave) 
					log.info(home+File.separator+key+" .txt saved sucessfully"); 
				else 
					log.error("Failed to save "+ home+File.separator+key+".txt"); 
			} 
		} 
		FileSystem hdfs =null; 
		Configuration conf1 = new Configuration(); 
		String home = System.getProperty("user.home"); 
		try{ 
			hdfs = FileSystem.get( new URI( filesPath.HDFS_URL ), conf1); 
			for(String key :keywords){ 
				Path localFilePath = new Path(home+File.separator+key+".txt"); 
				Path modelPath = new Path("/user/dev11"+File.separator+key+".txt"); 
				if (hdfs.exists(modelPath)) { 
					hdfs.delete(new Path("/user/dev11"+File.separator+key+".txt"), true); 
				} 
				hdfs.copyFromLocalFile(localFilePath, modelPath); 
			} 
		}catch(IOException e){ 
			e.printStackTrace(); 
		} catch (URISyntaxException e) { 
			e.printStackTrace(); 
		} 
	}

	public static void getUsersTimeLine(String[] fpaths){ 
		Twitter twitter = getTwitterInstance();
		JavaSparkContext ctx=null; try{ 
			ctx = new JavaSparkContext( new SparkConf().setAppName("Sentiment Analysis") .setMaster("local"));
			JavaRDD<String> lines = CommonFunctions.getJavaRDDFromFile(ctx, fpaths[0]);
			JavaRDD<String> flines = lines.filter( new Function<String, Boolean>() {
				public Boolean call(String s) throws Exception { 
					return !s.equalsIgnoreCase("Twitter_Id"); 
				} 
			} 
					); 
			List<String> ids = flines.collect(); 
			for(String key : ids){ 
				boolean fileSave = false; 
				String home = System.getProperty("user.home"); 
				try { 
					// write data into local file 
					File file = new File(home+File.separator+key+".txt"); 
					if(file.exists()){ 
						log.info("File "+home+File.separator+key+".txt" + " already exists"); 
						file.delete(); 
						log.info("File "+home+File.separator+key+".txt" + " deleted"); 
					} 
					if (!file.exists()) { 
						file.createNewFile(); 
						log.info("File "+home+File.separator+key+".txt" + " created"); 
					} 
					BufferedWriter writer = new BufferedWriter(new FileWriter(file)); 
					int numberOfTweets = 1000; 
					long lastID = Long.MAX_VALUE; 
					Paging page = new Paging(1,200); 
					ArrayList<Status> tweets = new ArrayList<Status>(); 
					while (tweets.size () < numberOfTweets) { 
						List<Status> statuses = twitter.getUserTimeline(Long.parseLong(key),page); 
						if(statuses.size() <1) 
							break; 
						tweets.addAll(statuses); 
						for (Status status : statuses) { 
							//System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText()); 
							writer.write(status.getUser().getId()+"\t@" + status.getUser().getScreenName() + "\t" + status.getText()+"\n"); 
						} 
						writer.flush(); 
						page.setMaxId(lastID-1); 
						Thread.sleep(65000l); } 
					writer.close(); 
					fileSave = true;
				} catch (TwitterException te) { 
					te.printStackTrace(); 
					log.error("Failed to get timeline: " + te.getMessage()); 
				}catch (IOException e) { 
					fileSave = false; 
					e.printStackTrace(); 
				} catch (InterruptedException e) { 
					// TODO Auto-generated catch block
					e.printStackTrace(); 
				} finally{ 
					if(fileSave) 
						log.info(home+File.separator+key+" .txt saved sucessfully"); 
					else 
						log.error("Failed to save "+ home+File.separator+key+".txt");
				} 
			} 
			FileSystem hdfs =null; 
			Configuration conf = new Configuration(); 
			String home = System.getProperty("user.home"); 
			try{ 
				hdfs = FileSystem.get( new URI( filesPath.HDFS_URL ), conf); 
				for(String key :ids){ 
					Path localFilePath = new Path(home+File.separator+key+".txt"); 
					Path modelPath = new Path("/user/dev11/Tweets/keywords"+File.separator+key+".txt"); 
					if (hdfs.exists(modelPath)) { 
						hdfs.delete(new Path("/user/dev11/Tweets/keywords"+File.separator+key+".txt"), true); 
					} 
					hdfs.copyFromLocalFile(localFilePath, modelPath); 
				} 
			}catch(IOException e){ 
				e.printStackTrace(); 
			} catch (URISyntaxException e) { 
				e.printStackTrace(); 
			} 
		}catch(Exception e){ 
			e.printStackTrace(); 
		} 
	} 

	public static void main(String[] args) { 
		List<String> keywords = new ArrayList<String>(); 
		keywords.add("Outlook.com"); 
		keywords.add("Bing"); 
		keywords.add("OneDrive"); 
		keywords.add("MSN"); 
		keywords.add("Microsoft Azure"); 
		keywords.add("Windows Live"); 
		keywords.add("HomeOS");
		keywords.add("Windows Media Player"); 
		keywords.add("Age of Empires"); 
		keywords.add("Age of Mythology"); 
		keywords.add("Dead Rising 3"); 
		keywords.add("Windows 10"); 
		keywords.add("Windows 7"); 
		keywords.add("Windows Server 2012"); 
		keywords.add("Code-view");
		keywords.add("AutoCollage 2008"); 
		keywords.add("Microsoft Expression Studio"); 
		keywords.add("Windows Live Movie Maker"); 
		keywords.add("Skype"); 
		keywords.add("Microsoft Silverlight"); 
		keywords.add("So.cl"); 
		keywords.add("Windows Live Mail"); 
		keywords.add("Office 365"); 
		keywords.add("Microsoft Access"); 
		keywords.add("Microsoft OneNote"); 
		keywords.add("Microsoft Office"); 
		keywords.add("Microsoft Visio"); 
		keywords.add("Microsoft SharePoint Workspace"); 
		keywords.add("Microsoft Project"); 
		keywords.add("Microsoft Publisher"); 
		keywords.add("Microsoft Lync"); 
		Calendar calendar = Calendar.getInstance(); 
		calendar.set(Calendar.YEAR, 2014); 
		calendar.set(Calendar.MONTH, 8); 
		calendar.set(Calendar.DAY_OF_MONTH, 10); // new years eve 
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 
		TwitterUtil.searchTweets(keywords, format.format(calendar.getTime()));
		/*String[] fpaths = {"file:///home/dev11/work/twitter_ids_full.csv","hdfs://localhost:9000/user/dev11/"}; 
		TwitterUtil.getUsersTimeLine(fpaths); */
	} 
}
