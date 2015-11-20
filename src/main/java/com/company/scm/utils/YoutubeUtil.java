package com.company.scm.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cafyne.common.property.cache.CafyneConfig;
import com.cafyne.dataaccess.model.datasift.Author;
import com.cafyne.dataaccess.model.datasift.DataSiftData;
import com.cafyne.dataaccess.model.datasift.Interaction;
import com.cafyne.dataaccess.model.datasift.Youtube;
import com.cafyne.dataaccess.model.mongo.PostData;
import com.cafyne.dataaccess.util.DateUtil;
import com.cafyne.youtube.db.connection.DBUtil;
import com.cafyne.youtube.model.YoutubePostData;
import com.cafyne.youtube.util.YoutubeProfileUpdates;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.YouTube.Captions.Download;
import com.google.api.services.youtube.model.Caption;
import com.google.api.services.youtube.model.CaptionListResponse;
import com.google.api.services.youtube.model.CaptionSnippet;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;

public class YoutubeUtil implements Callable<YoutubePostData>{

	private Date lastUpdatedTime;
	private static Logger logger = LoggerFactory.getLogger(YoutubeProfileUpdates.class);
	private final String channelId;
	private final String refresh_token;
	long wait;
	private final long min_wait = 1024;
	private final long max_wait = Long.valueOf(CafyneConfig.getProperty("LINKEDIN_MAX_WAIT"));
//	private DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	public static final JsonFactory JSON_FACTORY = new JacksonFactory();

	private static final String CREDENTIALS_DIRECTORY = ".oauth-credentials";

	private static com.google.api.services.youtube.YouTube youtube;

//	SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	public YoutubeProfileUpdates(String channelId, String refresh_token, 
			Date lastUpdatedTime, long wait) {
		this.channelId = channelId;
		this.lastUpdatedTime = lastUpdatedTime;
		this.wait = wait;
		this.refresh_token = refresh_token;
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	}


	@Override
	public YoutubePostData call() throws Exception {
		boolean changed = false;
		YoutubePostData youtubePostData = null;//
		Credential credential = new GoogleCredential.Builder()
		.setTransport(new NetHttpTransport())
		.setJsonFactory(JacksonFactory.getDefaultInstance())
		.setClientSecrets(CafyneConfig.getProperty("YOUTUBE_CLIENT_ID"), CafyneConfig.getProperty("YOUTUBE_CLIENT_SECRET"))
		.build().setFromTokenResponse(new TokenResponse().setRefreshToken(refresh_token));
		credential.refreshToken();
		String access_token = credential.getAccessToken();
		long count = 20;
		try{
			PostData postData = null;
			changed = false;
			boolean updated = false;
			Stack<DataSiftData> stack = new Stack<DataSiftData>();
			do{	
				YouTube youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName("youtube-video-details").build();
				YouTube.Search.List search = youtube.search().list("id,snippet");
				search.setChannelId(channelId);
				search.setOauthToken(access_token);
				search.setOrder("date");
				search.setMaxResults(count);
				SearchListResponse searchResponse = search.execute();
				System.out.println(" search result "+ searchResponse);
				stack = new Stack<DataSiftData>();
				List<SearchResult> searchResultList = searchResponse.getItems();
				if (searchResultList != null) {
					Iterator<SearchResult> it = searchResultList.iterator();
					while(it.hasNext()){
						SearchResult searchResult = it.next();
						Date d1 = sdf.parse(searchResult.getSnippet().getPublishedAt().toString());
						Date d2 = lastUpdatedTime!=null?DateUtil.addSecsToDate(lastUpdatedTime,1):null;
						if(lastUpdatedTime == null || d1.after(d2)){
							changed=true;
							DataSiftData data = generateDataSiftData(searchResult, youtube, access_token, credential);
//							System.out.println(" datasiftdata "+ data);
							if(null != data)
								stack.push(data);
						}else
							break;
					}
				}
				updated = false;
			}while(updated);
			youtubePostData = new YoutubePostData();
			List<DataSiftData> list = new ArrayList<DataSiftData>();
			while(null != stack && !stack.isEmpty()){
				DataSiftData data = null;
				if(stack.size()==1){
					data = stack.pop();
					lastUpdatedTime = sdf.parse(data.interaction.created_at);
				}else{
					data = stack.pop();
				}
				if(null != data)
					list.add(data);	
			}
			DBUtil.getInstance().updateLastPostUpdateTime(channelId, lastUpdatedTime);
			youtubePostData.setList(list);
			if(changed)
				wait = Math.max(min_wait, wait/2);
			else
				wait = Math.min(max_wait, wait*2);
			youtubePostData.setWait(wait);
		} catch(Exception e){
			e.printStackTrace();
			logger.error(" Youtube Client Exception "+ e.getMessage());
			wait = DateUtil.truncateTime(DateUtil.addDaysToDate(new Date(), 1)) - System.currentTimeMillis();
			youtubePostData = new YoutubePostData();
			youtubePostData.setWait(wait);
			return youtubePostData;
		}
		return youtubePostData;
	}

	private DataSiftData generateDataSiftData(SearchResult searchResult, com.google.api.services.youtube.YouTube youtube, String access_token, Credential credential) throws ParseException {
		DataSiftData dataSiftData = null;
		if(searchResult.getId().getKind().equalsIgnoreCase("youtube#video")){
			dataSiftData = new DataSiftData();
			dataSiftData.interaction = new Interaction();
			dataSiftData.interaction.id = searchResult.getId().getVideoId();
			dataSiftData.interaction.content = searchResult.getSnippet().getTitle();
			dataSiftData.interaction.created_at = searchResult.getSnippet().getPublishedAt().toString();
			dataSiftData.interaction.type = "youTube";
			dataSiftData.interaction.author = new Author();
			dataSiftData.interaction.author.id = searchResult.getSnippet().getChannelId();
			dataSiftData.interaction.author.name = searchResult.getSnippet().getChannelTitle();
			dataSiftData.interaction.author.username = searchResult.getSnippet().getChannelTitle();
//			dataSiftData.interaction.author.avatar = searchResult.getSnippet().getThumbnails().getMedium().getUrl();
			dataSiftData.interaction.keywords = "";
			dataSiftData.interaction.link = getYoutubeLink(searchResult.getId().getVideoId());
			dataSiftData.interaction.source = searchResult.getSnippet().getThumbnails().getMedium().getUrl();
			dataSiftData.youtube = new Youtube();
			dataSiftData.youtube.id = searchResult.getId().getVideoId();
			dataSiftData.youtube.message = searchResult.getSnippet().getDescription();
			dataSiftData.youtube.title = searchResult.getSnippet().getTitle();
			dataSiftData.youtube.profileHeadline = searchResult.getSnippet().getChannelTitle();
			dataSiftData.youtube.linkUrl = getYoutubeLink(searchResult.getId().getVideoId());
			dataSiftData.youtube.publishedTime = searchResult.getSnippet().getPublishedAt().toString();
			List<Caption> list = getCaptionList(credential, searchResult.getId().getVideoId(), access_token);
			if(null != list && !list.isEmpty()){
				dataSiftData.youtube.subtitle = getCaption(credential, list.get(0).getId(), access_token);
			}
		}
		return dataSiftData;
	}

	private String getYoutubeLink(String videoId) {
		StringBuffer sb = new StringBuffer();
		String url = "https://www.youtube.com";
		sb.append(url);
		sb.append(File.separator);
		sb.append("watch");
		sb.append("?");
		sb.append("v");
		sb.append("=");
		sb.append(videoId);
		return sb.toString();
	}


	private String getCaption(Credential credential, String captionId, String access_token) {
		String retValue = null;
//		HttpURLConnection connection = null;
//		InputStream inputStream = null;
//		String charset = "UTF-8";
		try{
			
			/*Credential credential2 = new GoogleCredential.Builder()
	        .setTransport(new NetHttpTransport())
	        .setJsonFactory(JacksonFactory.getDefaultInstance())
	        .setClientSecrets(CafyneConfig.getProperty("YOUTUBE_CLIENT_ID"), CafyneConfig.getProperty("YOUTUBE_CLIENT_SECRET"))
	        .build().setFromTokenResponse(new TokenResponse().setRefreshToken(refresh_token));
			credential2.getRefreshToken();
			String access_token1 = credential2.getAccessToken();*/
			
			/*StringBuffer sb = new StringBuffer();
			sb.append("https://www.googleapis.com/youtube/v3/captions/");
			sb.append(captionId);
			sb.append("?");
			sb.append("key");
			sb.append("=");
			sb.append("AIzaSyCFj15TpkchL4OUhLD1Q2zgxQnMb7v3XaM");
			
			HttpClient client = new DefaultHttpClient();
			HttpGet request = new HttpGet(sb.toString());

			// add request header
			request.addHeader("authorization", DatatypeConverter.printBase64Binary(access_token1.getBytes()));

			HttpResponse response = client.execute(request);
			
			System.out.println("   "+ response.getStatusLine().getStatusCode());
			
			connection = getConnection("https://content.googleapis.com/youtube/v3/captions/", captionId ,access_token1);
			inputStream = connection.getInputStream();
			int responseCode = connection.getResponseCode();

			if (responseCode >= 200 && responseCode <= 299) {

				BufferedReader reader = new BufferedReader(new InputStreamReader((inputStream), charset));
				String line = reader.readLine();
				while(line != null){
					System.out.println("    "+line);
				}
			}
			// Create an API request to the YouTube Data API's captions.download
			// method to download an existing caption track.
			/*Credential credential2 = new GoogleCredential.Builder()
	        .setTransport(new NetHttpTransport())
	        .setJsonFactory(JacksonFactory.getDefaultInstance())
	        .setClientSecrets(CafyneConfig.getProperty("YOUTUBE_CLIENT_ID"), CafyneConfig.getProperty("YOUTUBE_CLIENT_SECRET"))
	        .build().setFromTokenResponse(new TokenResponse().setRefreshToken(refresh_token));
			credential.getRefreshToken();
			String access_token1 = credential.getAccessToken();
			YouTube youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential2)
			.setApplicationName("youtube-cmdline-commentthreads-sample").build();*/
			YouTube youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY,  credential)
			.setApplicationName("youtube-cmdline-commentthreads-sample").build();
			Download captionDownload = youtube.captions().download(captionId).setTfmt("srt");
			//			captionDownload.setOauthToken(access_token1);
			//			captionDownload.setKey("AIzaSyDxJH9a1ViEm3B1qcef9oDiCcrGI62ShPg");
			// Set the download type and add an event listener.
			MediaHttpDownloader downloader = captionDownload.getMediaHttpDownloader();

			// Indicate whether direct media download is enabled. A value of
			// "True" indicates that direct media download is enabled and that
			// the entire media content will be downloaded in a single request.
			// A value of "False," which is the default, indicates that the
			// request will use the resumable media download protocol, which
			// supports the ability to resume a download operation after a
			// network interruption or other transmission failure, saving
			// time and bandwidth in the event of network failures.
			downloader.setDirectDownloadEnabled(false);

			// Set the download state for the caption track file.
			MediaHttpDownloaderProgressListener downloadProgressListener = new MediaHttpDownloaderProgressListener() {
				@Override
				public void progressChanged(MediaHttpDownloader downloader) throws IOException {
					switch (downloader.getDownloadState()) {
					case MEDIA_IN_PROGRESS:
						System.out.println("Download in progress");
						System.out.println("Download percentage: " + downloader.getProgress());
						break;
						// This value is set after the entire media file has
						//  been successfully downloaded.
					case MEDIA_COMPLETE:
						System.out.println("Download Completed!");
						break;
						// This value indicates that the download process has
						//  not started yet.
					case NOT_STARTED:
						System.out.println("Download Not Started!");
						break;
					}
				}
			};
			downloader.setProgressListener(downloadProgressListener);
			//			AmazonS3 s3client = new AmazonS3Client( new BasicAWSCredentials("AKIAJVRRWE2MFZAAKV7A", "IzNXAfDetAHoG1GaPKa3cHwG1Hfbv3purDEHDxBQ"));
			//			String keyName = "Youtube_Video_"+videoId+"_"+captionId;
			OutputStream outputFile = new OutputStream()
			{
				private StringBuilder string = new StringBuilder();
				@Override
				public void write(int x) throws IOException {
					this.string.append((char) x );
				}

				public String toString(){
					return this.string.toString();
				}
			};
			// Download the caption track.
			captionDownload.executeAndDownloadTo(outputFile);
			String subtitle = outputFile.toString();
			if(null != subtitle && !subtitle.isEmpty()){
				String cstring1 = Pattern.compile("(?m)^(\\s*\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d\\s+)").matcher(subtitle.replaceAll("-->", "")).replaceAll("");
				String cstring2 = Pattern.compile("(?m)^(\\s*\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d\\n)").matcher(cstring1).replaceAll("");
				String cstring3 = Pattern.compile("\\d?\\d").matcher(cstring2).replaceAll("");
				String fstring = cstring3.replaceAll("\n\n", "");
				retValue = fstring;
			}
			//			s3client.putObject(new PutObjectRequest("datasift-feed-avinash-local", keyName, scratchFile));
		}catch(Exception e){
			e.printStackTrace();
		}
		return retValue;
	}


	private HttpURLConnection getConnection(String url, String captionId,
			String access_token) {

		StringBuffer sb = new StringBuffer();
		sb.append(url);
		sb.append(captionId);
		sb.append("?");
		sb.append("key");
		sb.append("=");
		sb.append("AIzaSyCFj15TpkchL4OUhLD1Q2zgxQnMb7v3XaM");
		URL url1;
		HttpURLConnection connection = null;
		try {
			url1 = new URL(sb.toString());

			connection = (HttpURLConnection) url1.openConnection();
			connection.setReadTimeout(1000 * 60 * 60);
			connection.setConnectTimeout(1000 * 10);
			connection.setRequestProperty("authorization", createAuthHeader(access_token));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}


	private String createAuthHeader(String access_token) {
		return DatatypeConverter.printBase64Binary(access_token.getBytes());
	}


	public List<Caption> getCaptionList(Credential credential, String videoId, String access_token){
		List<Caption> fcaptions = null;
		try {
			//			WebView webview = new WebView(this);

			/*	FileDataStoreFactory fileDataStoreFactory = new FileDataStoreFactory(new File(System.getProperty("user.home") + "/" + CREDENTIALS_DIRECTORY));
			DataStore<StoredCredential> datastore = fileDataStoreFactory.getDataStore("captions");
			List<String> scopes = Lists.newArrayList("https://www.googleapis.com/auth/youtube.force-ssl");
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
					CafyneConfig.getProperty("YOUTUBE_CLIENT_ID_1"), CafyneConfig.getProperty("YOUTUBE_CLIENT_SECRET_1"), scopes).setCredentialDataStore(datastore)
					.build();

			// Build the local server and bind it to port 8080
			LocalServerReceiver localReceiver = new LocalServerReceiver.Builder().setPort(8081).build();

			// Authorize.
			Credential credential1 =  new AuthorizationCodeInstalledApp(flow, localReceiver).authorize("user");*/
			/*Credential credential2 = new GoogleCredential.Builder()
	        .setTransport(new NetHttpTransport())
	        .setJsonFactory(JacksonFactory.getDefaultInstance())
	        .setClientSecrets(CafyneConfig.getProperty("YOUTUBE_CLIENT_ID"), CafyneConfig.getProperty("YOUTUBE_CLIENT_SECRET"))
	        .build().setAccessToken(access_token);*/
			//			credential.refreshToken();
			//			String access_token1 = credential2.getAccessToken();

			YouTube youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, new HttpRequestInitializer() {
				public void initialize(HttpRequest request)
						throws IOException {
				}
			})
			.setApplicationName("youtube-cmdline-commentthreads-sample").build();
			CaptionListResponse captionListResponse = youtube.captions().
					list("snippet", videoId)/*.setOauthToken(access_token)*/.setKey("AIzaSyBBcDIZNRLnUVz2i_YQH6f9d6zh3FIuo74").execute();
			List<Caption> captions = captionListResponse.getItems();
			// Print information from the API response.
			System.out.println("\n================== Returned Caption Tracks ==================\n");
			CaptionSnippet snippet;
			for (Caption caption : captions) {
				snippet = caption.getSnippet();
				System.out.println("  - ID: " + caption.getId());
				System.out.println("  - Name: " + snippet.getName());
				System.out.println("  - Language: " + snippet.getLanguage());
				System.out.println("\n-------------------------------------------------------------\n");
				//				downloadCaption(credential, caption.getId(), videoId);
				if(snippet.getLanguage().equalsIgnoreCase("en")){
					if(null == fcaptions)
						fcaptions = new ArrayList<Caption>();
					fcaptions.add(caption);
				}
			}

		} catch (GoogleJsonResponseException e) {
			logger.error("GoogleJsonResponseException code: " + e.getDetails().getCode()
					+ " : " + e.getDetails().getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (Throwable t) {
			logger.error("Throwable: " + t.getMessage());
			t.printStackTrace();
		}
		return fcaptions;
	}

}
