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
import java.util.Random;

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
	public void getTwitterFriendsList(String ipath, String opath, int depth1, int depth2, int depth3) throws IOException{
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
				sb = new StringBuffer();
				long cursor =-1l;
				IDs friendlist = null;
				if(list.contains(query))
					continue;
				try{
					int depth_1 = 0;
					do {
						friendlist = twitter.getFriendsIDs(Long.valueOf(query), cursor);
						long[] ids = friendlist.getIDs();
						for(int i=0; i< ids.length; i++){
							Random random = new Random();
							if(random.nextDouble() <= 0.8 )
								continue;
							sb.append(words[0]);
							sb.append(" ");
							sb.append(ids[i]);
							sb.append("\n");
							Thread.sleep(65000l);
							long cursor_2 = -1;
							int depth_2 = 0;
							IDs friendslist_2 = null;
							do{
								friendslist_2 = twitter.getFriendsIDs(ids[i], cursor_2);
								long[] ids_2 = friendslist_2.getIDs();
								for(int j=0; j < ids_2.length; j++){
									Random random_2 = new Random();
									if(random_2.nextDouble() <= 0.9)
										continue;
									sb.append(ids[i]);
									sb.append(" ");
									sb.append(ids_2[j]);
									sb.append("\n");
									Thread.sleep(130000l);
									long cursor_3 = -1;
									int depth_3 = 0;
									IDs friendslist_3 = null;
									do{
										friendslist_3 = twitter.getFriendsIDs(ids_2[j], cursor_3);
										long[] ids_3 = friendslist_3.getIDs();
										for(int k=0; k < ids_3.length; k++){
											Random random_3 = new Random();
											if(random_3.nextDouble() <= 0.95)
												continue;
											sb.append(ids_2[j]);
											sb.append(" ");
											sb.append(ids_3[k]);
											sb.append("\n");
											depth_3++;
											if(depth_3 > depth3)
												break;
										}
										Thread.sleep(65000l);
									}while((cursor_3 = friendslist_3.getNextCursor())!=0 && depth_3 < depth3);
								depth_2++;
								if(depth_2 > depth2)
									break;
								}
								Thread.sleep(80000l);
							}while((cursor_2 = friendslist_2.getNextCursor())!=0 && depth_2 < depth2);
							depth_1++;
							if(depth_1 > depth1)
								break;
						}
//						System.out.println(" user's friends list "+ sb);
						Thread.sleep(900001l);
					} while((cursor = friendlist.getNextCursor())!=0 && depth_1 < depth1);
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
			adjList.getTwitterFriendsList("/home/hduser/work/profile_list_2.csv", "/home/hduser/work/friendslist_3.csv", 100, 10, 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
