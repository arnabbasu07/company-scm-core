package com.company.kol.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import twitter4j.IDs;
import twitter4j.PagableResponseList;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;

import com.company.scm.utils.SocialNetworkUtils;
import com.google.gson.Gson;

public class AdjacencyList {
	public void getTwitterFriendsList(String ipath, String opath) throws IOException{
		Configuration conf = SocialNetworkUtils.createTwitterConfiguration();
		TwitterFactory twitterFactory = new TwitterFactory(conf);
		Twitter twitter = twitterFactory.getInstance();
		File file = new File(opath);
		if (!file.exists()) {
			file.createNewFile();
		}
		//		Path path = fs.getPath(first, more)new Path("/home/cafyne/work/TwitterProfiles.csv");
		try{
			BufferedReader br = new BufferedReader(new FileReader(ipath));
			BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
			String str ;
			StringBuffer sb = null;
			List<String> list=new ArrayList<String>();
			while((str = br.readLine())!=null){
				String[] words = StringUtils.splitPreserveAllTokens(str, ",");
				String query = words[0];
				sb = new StringBuffer(query);
				long cursor =-1l;
				IDs friendlist = null;
				if(list.contains(query))
					continue;
				try{
					do {
						friendlist = twitter.getFriendsIDs(Long.valueOf(query), cursor);
						long[] ids = friendlist.getIDs();
						for(int i=0; i< ids.length; i++){
							sb.append(",");
							sb.append(ids[i]);
						}
						System.out.println(" user's friends list "+ sb);
						Thread.sleep(65000l);
					} while((cursor = friendlist.getNextCursor())!=0);
					Thread.sleep(65000l);
				}catch(Exception e){
					e.printStackTrace();
				}
				System.out.println(" User Profile "+sb);
				bw.write(sb+"\n");
				bw.flush();
				list.add(query);
			}
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		AdjacencyList adjList = new AdjacencyList();
		try {
			adjList.getTwitterFriendsList("/home/hduser/work/profile_Id_list_1.csv", "/home/hduser/work/friendslist_1.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
