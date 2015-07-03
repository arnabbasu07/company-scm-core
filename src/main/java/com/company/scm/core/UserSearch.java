package com.company.scm.core;

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

import com.company.scm.utils.SocialNetworkUtils;
import com.google.gson.Gson;

import facebook4j.Facebook;
import facebook4j.FacebookException;
import facebook4j.FacebookFactory;
import twitter4j.PagableResponseList;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;


public class UserSearch {

	public void getTwitterUsers(String ipath, String opath) throws IOException{
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
			while((str = br.readLine())!=null){
				String[] words = StringUtils.splitPreserveAllTokens(str, ",");
				String query = words[0]+(words[1].equalsIgnoreCase("null")?" ":" "+words[1])+" "+words[2];
				//				System.out.println(" query "+query);
				ResponseList<User> response = twitter.searchUsers(query, 3);
				System.out.println(" # users "+response.size());
				if(null != response && response.size() > 0){
					Iterator<User> it = response.iterator();
					while(it.hasNext()){
						sb = new StringBuffer();
						User user = it.next();
						sb.append(",");
						sb.append(words[words.length-1]);
						sb.append(",");
						sb.append(user.getName());
						sb.append(",");
						sb.append("");
						sb.append(",");
						sb.append("");
						sb.append(",");
						sb.append("");
						sb.append(",");
						sb.append(user.getLocation().replaceAll(",", "#"));
						sb.append(",");
						sb.append("");
						sb.append(",");
						sb.append((user.getLang()==null?"":user.getLang().replaceAll(",", "#")));
						sb.append(",");
						sb.append((user.getTimeZone()==null?"":user.getTimeZone().replaceAll(",", "#")));
						long cursor =-1l;
						PagableResponseList<User> friendlist = null;
						List<String> list=new ArrayList<String>();
						try{
							do {
								friendlist = twitter.getFriendsList(user.getId(), cursor);
								Iterator<User> friends = friendlist.iterator();
								while(friends.hasNext()){
									list.add(friends.next().getName());
								}
								Thread.sleep(65000l);
							} while((cursor = friendlist.getNextCursor())!=0 && list.size() < 100);
						}catch(Exception e){
							e.printStackTrace();
						}
						sb.append(",");
						sb.append(new Gson().toJson(list));
						sb.append(",");
						sb.append(user.getId());
						System.out.println(" User Profile "+sb);
						bw.write(sb.substring(1)+"\n");
						bw.flush();
					}
					Thread.sleep(65000l);
				}
			}
		}catch(TwitterException te){
			te.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}

	public void getFacebookUsers(String ipath, String opath) throws IOException{
		facebook4j.conf.Configuration conf = SocialNetworkUtils.createFacebookConfiguration();
		FacebookFactory facebookFactory = new FacebookFactory(conf);
		Facebook facebook = facebookFactory.getInstance();
		File file = new File(opath);
		if (!file.exists()) {
			file.createNewFile();
		}
		try{
			BufferedReader br = new BufferedReader(new FileReader(ipath));
			BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
			String str ;
			StringBuffer sb;
			while((str = br.readLine())!=null){
				String[] words = StringUtils.splitPreserveAllTokens(str, ",");
				String query = words[0]+(words[1].equalsIgnoreCase("null")?" ":" "+words[1])+" "+words[2];
				System.out.println(" query "+query);
				facebook4j.ResponseList<facebook4j.User> response = facebook.searchUsers(query);
				if(null != response){
					Iterator<facebook4j.User> it = response.iterator();
					while(it.hasNext()){
						sb = new StringBuffer();
						facebook4j.User user = it.next();
						sb.append(words[words.length -1]);
						sb.append(",");
						if(user.getFirstName()!=null && user.getLastName()!=null){
							sb.append(user.getFirstName()==null?"":user.getFirstName());
							sb.append(",");
							sb.append(user.getMiddleName()==null?"":user.getMiddleName());
							sb.append(",");
							sb.append(user.getLastName()==null?"":user.getLastName());
						}else{
							sb.append(user.getName());
						}
						sb.append(",");
						sb.append("");
						sb.append(",");
						sb.append(user.getGender()==null?"":user.getGender());
						sb.append(",");
						sb.append(user.getBio()==null?"":user.getBio());
						sb.append(",");
						sb.append(user.getLocation()==null?"":user.getLocation());
						sb.append(",");
						sb.append(user.getEmail()==null?"":user.getEmail());
						sb.append(",");
						sb.append(user.getLanguages()==null?"":user.getLanguages());
						System.out.println(" user "+ sb.toString()+" "+user.getAgeRange()+" "+user.getBirthday()+" "+user.getHometown()+" "+user.getWork()+" "+user.getEducation()+" "+user.getFavoriteAthletes()+" "+user.getFavoriteTeams()+" "+user.getTimezone()
								+" "+user.getRelationshipStatus()+" "+user.getQuotes()
								+" "+user.getHometown()+" "+user.getWebsite());
					}
				}
				Thread.sleep(30000l);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (FacebookException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
	public static void main(String[] args) {
		UserSearch tSearch = new UserSearch();
		try {
			tSearch.getTwitterUsers("/home/cafyne/work/Employees_cafyne.csv","/home/cafyne/work/TwitterProfiles_cafyne_2.csv");
			//			tSearch.getFacebookUsers("/home/cafyne/work/Employees_cafyne.csv","/home/cafyne/work/FacebookProfiles.csv");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
