package com.company.scm.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Stack;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import com.cafyne.dataaccess.model.datasift.DataSiftData;
import com.cafyne.dataaccess.model.mongo.PostData;
import com.cafyne.dataaccess.util.DateUtil;
import com.cafyne.linkedin.db.connection.DBUtil;
import com.cafyne.linkedin.model.LinkedInNetwork;
import com.cafyne.linkedin.model.LinkedInPostData;
import com.google.gson.Gson;

public class LinkedInUtil {

	public LinkedInPostData call() throws Exception {
		HttpClient httpclient = new DefaultHttpClient();
		boolean changed = false;
		int start = 0, count = 10;
		LinkedInPostData linkedInPostData = null;//
		try{
			PostData postData = null;
			changed = false;
			boolean updated = false;
			Stack<DataSiftData> stack = new Stack<DataSiftData>();
			do{	
				HttpGet httpGet = new HttpGet(getCompanyUpdateURL(start, count));
				HttpResponse response2 = httpclient.execute(httpGet);
				HttpEntity entity = response2.getEntity();
				if(entity == null)
					continue;
				String response1 = EntityUtils.toString(entity);
				JSONObject jsonObject = new JSONObject(response1);
				logger.debug(" Following are updates for companyId "+ linkedInCompanyId + " "+jsonObject + "  "+response2.getStatusLine().getStatusCode());
				if(Integer.valueOf(response2.getStatusLine().getStatusCode())!=200)
					throw new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS);
				if(response1.isEmpty())
					break;
				Gson gson = new Gson();
				LinkedInNetwork linkedInNetwork = gson.fromJson(response1, LinkedInNetwork.class);
				logger.debug(" LinkedIn network "+ linkedInNetwork);
				int i;
				for(i=0; i< linkedInNetwork.getValues().size() ; i++){
					Date d1 = new Date(linkedInNetwork.getValues().get(i).getTimestamp());
					Date d2 = lastUpdatedTime!=null?DateUtil.addSecsToDate(lastUpdatedTime,1):null;
					if(lastUpdatedTime == null || d1.after(d2)){
						changed=true;
						stack.push(generateDataSiftData(linkedInNetwork.getValues().get(i)));
					}else
						break;
				}
				if(i<count)
					updated = false;
				else{
					start += count;
					updated = true;
				}
			}while(updated);
			linkedInPostData = new LinkedInPostData();
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
			DBUtil.getInstance().updateLastPostUpdateTime(linkedInCompanyId, lastUpdatedTime);
			linkedInPostData.setList(list);
			if(changed)
				wait = Math.max(min_wait, wait/2);
			else
				wait = Math.min(max_wait, wait*2);
			linkedInPostData.setWait(wait);
		}catch(HttpClientErrorException e){
			logger.error(" LinkedIn Client Exception "+ e.getMessage());
			wait = DateUtil.truncateTime(DateUtil.addDaysToDate(new Date(), 1)) - System.currentTimeMillis();
			linkedInPostData = new LinkedInPostData();
			linkedInPostData.setWait(wait);
			return linkedInPostData;
		} catch(Exception e){
			e.printStackTrace();
		}
		return linkedInPostData;
	}
}
