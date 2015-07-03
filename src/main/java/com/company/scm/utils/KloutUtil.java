package com.company.scm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.company.commons.core.Constants;
//import com.klout4java.Klout4Java;
//import com.klout4java.KloutConfig;
//import com.klout4java.vo.ScoreResponse;


public class KloutUtil {
/*
	public static Map<String,Double> getKloutScore(List<String> twitterids){
		KloutConfig config = new KloutConfig();
		config.setApiKey("2grzhg3x2r43qj8ax3sc4k98");
		Klout4Java klout = new Klout4Java();
		klout.setConfig(config);
		Map<String,Double> IdScoreMap = new HashMap<String, Double>();
		for(String twitterId :twitterids){
			try{
				String kloutID = klout.kloutIDForTwitterID(twitterId);
				ScoreResponse score = klout.score(kloutID);
				IdScoreMap.put(twitterId,Double.parseDouble(score.getScore()));
				Thread.sleep(10000l);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return IdScoreMap;
	}*/
	public static void main(String[] args) {
		List<String> twitterids = new ArrayList<String>(){{			
			add("29485383");
			add("33354412");
			add("33611242");
			add("118305616");
			add("19120022");
			add("36768592");
			add("22981369");
			add("230210312");
			add("6795782");
			add("9131582");
			add("320811209");
			add("387139097");
			add("16570124");
			add("269818506");
			add("807847837");
			add("224426027");
			add("50649100");
			add("2462029393");
			add("5619992");
			add("251410458");
			add("40894602");
			add("1113424424");
			add("701794538");
			add("55401506");
			add("164582687");
			add("47388608");
			add("38734668");
			add("274655871");
			add("311946941");
			add("209109237");
			add("209109237");
			add("209109237");
			add("90977040");
			add("15246063");
			add("500254607");
			add("7912462");
			add("828748471");
			add("552863346");
			add("100351490");
			add("15285067");
			add("6042442");
			add("79985427");
			add("35080292");
			add("141274160");
		}};
		/*Map<String,Double> kscores = getKloutScore(twitterids);
		for(String Id:twitterids){
			System.out.println(Id+","+(null==kscores.get(Id)?"NA":Constants.df.format(kscores.get(Id))));
		}*/
	}	
}
