package com.company.scm.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.twitter.Regex;

public class TweetTextUtil {

	public static String checkAndRemoveSpecialCharacters(
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
	public static String checkAndRemoveRetweet(String s){
		Pattern pattern = Pattern.compile("\\b\\s(RT|rt)\\s*@(\\w+|\\d+):\\s*\\b",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed Retweet  "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	public static String checkAndRemoveatRateUser(String s){
		Pattern pattern = Pattern.compile("\\b\\s*@(\\w+|\\d+|\\w+\\d+|\\w+_\\w+|\\w+_\\d+)");
		Matcher matcher = pattern.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed @user "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	public static String checkAndRemoveHashtag(String s){
		Matcher matcher =  Regex.VALID_HASHTAG.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed #tag "+matcher.replaceAll(""));
		return matcher.replaceAll("");
	}

	public static String checkAndRemoveURL(String s){
		Matcher matcher =  Regex.VALID_URL.matcher(s);
		if(matcher.matches())
			System.out.println(" Removed url "+ matcher.replaceAll(""));
		return matcher.replaceAll("");
	}
}
