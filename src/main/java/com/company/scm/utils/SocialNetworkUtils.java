package com.company.scm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import facebook4j.Facebook;
import facebook4j.FacebookFactory;
import facebook4j.conf.Configuration;
import facebook4j.conf.ConfigurationBuilder;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.RequestToken;

public class SocialNetworkUtils {

	private static final Logger log = LoggerFactory
			.getLogger(SocialNetworkUtils.class);

	public twitter4j.auth.AccessToken createTwitterAccessToken(
			String oauthVerifier) {
		twitter4j.conf.ConfigurationBuilder confBuilder = new twitter4j.conf.ConfigurationBuilder();
		confBuilder
		.setDebugEnabled(true)
		.setOAuthConsumerKey(
				"MVPLnA0sa07c5zVlwTB9A")
				.setOAuthConsumerSecret(
						"aD5V69HHodXy65JRGIzGMHEboR6tVx7ctKp44x0YU");
		TwitterFactory twitterFactory = new TwitterFactory(confBuilder.build());
		Twitter twitterClient = twitterFactory.getInstance();
		RequestToken twitterRequestToken = null;
		twitter4j.auth.AccessToken twitterAccessToken = null;
		try {
			twitterRequestToken = twitterClient.getOAuthRequestToken();
			twitterAccessToken = twitterClient.getOAuthAccessToken(
					twitterRequestToken, oauthVerifier);

		} catch (TwitterException e) {
			log.error("Getting error form twitter while create access token"
					+ e.getMessage());
		}
		return twitterAccessToken;
	}

	public static Configuration createFacebookConfiguration() {

		ConfigurationBuilder confBuilder = new ConfigurationBuilder();
		confBuilder.setDebugEnabled(true);
		confBuilder.setOAuthAppId("358300034293206");
		//358300034293206
		confBuilder.setOAuthAppSecret("c6f5e1338eec1d759934cd9670e30b77");
		//c6f5e1338eec1d759934cd9670e30b77
		confBuilder.setOAuthPermissions("user_status,public_profile,user_friends,manage_pages,publish_actions");
		confBuilder.setUseSSL(true);
		confBuilder.setJSONStoreEnabled(true);
		confBuilder.setOAuthAccessToken("CAAVJnyl61mkBAPrfa8Hcxafowkmkn8TBX4QPcqIZBdpj6MQ7xHZBkfdv6jWsWVcB6siZCq6cz5bBabxFd5fb88MZAhG6mDNciX4gYtXCMEZABfwuDCqIpyZCvxssWu61dnzrkjzvDNICMQsuR05PLleyBTxvkA8FrMqBAIN6EmS0Bdxm7mJz3qBvs3CA4mgiiJVamkzwBmj43p82QmDvJF");
//		confBuilder.setOAuthAuthorizationURL(oAuthAuthorizationURL);
//		confBuilder.setClientURL("http://localhost:8080/cafyneapp.web/facebookCallback");
//		confBuilder.setOAuthCallbackURL("http://fbsubscribedev.cafyne.net/FacebookRealTime/rest/facebook/subscriptionCallBack");
		return confBuilder.build();
	}

	public static twitter4j.conf.Configuration createTwitterConfiguration() {
		twitter4j.conf.ConfigurationBuilder confBuilder = new twitter4j.conf.ConfigurationBuilder();
		confBuilder
		.setDebugEnabled(true)
		.setOAuthConsumerKey(
				"MVPLnA0sa07c5zVlwTB9A")
				.setOAuthConsumerSecret(
						"aD5V69HHodXy65JRGIzGMHEboR6tVx7ctKp44x0YU")
						.setOAuthAccessToken("1466293950-1sWqxLJYABDAClMTrA6lxk8xdZd7oqlwxzL12yj")
						.setOAuthAccessTokenSecret("sXwcyA2gr47uEVFWbV6kgThQ1pO96za2xY7UmIoZhg");
		return confBuilder.build();
	}

	public twitter4j.conf.Configuration createTwitterConfiguration(String OAuthToken, String OAuthSecret) {
		twitter4j.conf.ConfigurationBuilder confBuilder = new twitter4j.conf.ConfigurationBuilder();
		confBuilder
		.setDebugEnabled(true)
		.setOAuthConsumerKey(
				"MVPLnA0sa07c5zVlwTB9A")
				.setOAuthConsumerSecret(
						"aD5V69HHodXy65JRGIzGMHEboR6tVx7ctKp44x0YU");
		confBuilder.setOAuthAccessToken(OAuthToken);
		confBuilder.setOAuthAccessTokenSecret(OAuthSecret);
		return confBuilder.build();
	}

	public Facebook getFacebookClient(String token) {
		facebook4j.auth.AccessToken accessToken;
		Configuration conf = createFacebookConfiguration();
		FacebookFactory facebookFactory = new FacebookFactory(conf);
		Facebook facebookClient = null;
		try {

			if(null != token){
				facebookClient = facebookFactory.getInstance();;
				accessToken = new facebook4j.auth.AccessToken(token);
				facebookClient.setOAuthAccessToken(accessToken);
				return facebookClient;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return facebookClient;
	}
}
