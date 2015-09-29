package com.company.scm.utils;

import java.io.BufferedWriter; 
import java.io.File; 
import java.io.FileNotFoundException;
import java.io.FileWriter; 
import java.io.IOException; 
import java.net.URI; 
import java.net.URISyntaxException; 
import java.net.URL;
import java.text.SimpleDateFormat; 
import java.util.ArrayList; 
import java.util.Arrays;
import java.util.Calendar; 
import java.util.List;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.spark.SparkConf; 
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.company.commons.core.CommonFunctions;
import com.company.commons.core.HadoopPaths;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLFetcher;
import twitter4j.Paging; 
import twitter4j.Query; 
import twitter4j.QueryResult; 
import twitter4j.Status; 
import twitter4j.Twitter; 
import twitter4j.TwitterException; 
import twitter4j.TwitterFactory; 
import twitter4j.URLEntity;
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

	public static void searchTweets(List<String> keywords,String date, String[] fpaths) throws IOException, URISyntaxException, InterruptedException{ 
		int max_tweets = 3000; 
		twitter4j.conf.Configuration conf = SocialNetworkUtils.createTwitterConfiguration();//getTwitterInstance(); 
		TwitterFactory twitterFactory = new TwitterFactory(conf);
		Twitter twitter = twitterFactory.getInstance();
		Configuration configuration = new Configuration();
		final FileSystem fs = FileSystem.get( new URI( filesPath.HDFS_URL ),configuration );
		Path fpath = new Path(fpaths[0]);
		if (fs.exists(fpath)) {
			fs.delete(fpath, true);			   
		}
		for(String key :keywords){ 
			boolean fileSave = false; 
			Path npath = new Path(fpaths[0]+File.separator+key);
			FSDataOutputStream out = fs.create(npath);
			StringBuffer fsb = new StringBuffer();
			try { 
				Query query = new Query(key.toLowerCase()); 
				query.setLang("en"); 
				query.setSince(date); 
				QueryResult result; 
				int count = 0; 
				fsb = new StringBuffer();
				do { 
					result = twitter.search(query); 
					List<Status> tweets = result.getTweets(); 
					for (Status tweet : tweets) { 
						StringBuffer sb = new StringBuffer();
						sb.append(tweet.getUser().getId());
						sb.append("\t");
						sb.append(tweet.getText());
						for(URLEntity entity :tweet.getURLEntities()){
							sb.append("\t");
							try{
								URL url =
										new URL(
												entity.getExpandedURL());
								final InputSource is = HTMLFetcher.fetch(url).toInputSource();
								final BoilerpipeSAXInput in = new BoilerpipeSAXInput(is);
								final TextDocument doc = in.getTextDocument();
								sb.append(ArticleExtractor.INSTANCE.getText(doc));
							} catch (SAXException e) {
								e.printStackTrace();
							} catch (BoilerpipeProcessingException e) {
								e.printStackTrace();
							} catch(FileNotFoundException e){
								e.printStackTrace();
							} catch(IOException e){
								e.printStackTrace();
							} 
						}	
						sb.append("\n");
						log.info(tweet.getUser().getId()+"\t"+tweet.getText()+"\t"+Arrays.asList(tweet.getURLEntities()));
						fsb.append(sb);
					} 
					count +=tweets.size(); 
					log.info(fsb.toString()+"   ");
					out.writeBytes(fsb.toString());
					Thread.sleep(65000l); 
				} while ((query = result.nextQuery()) != null && count < max_tweets); 
				fileSave = true; 
				out.close();
			} catch (TwitterException te) { 
				te.printStackTrace(); 
				log.info("Failed to search tweets: " + te.getMessage()); 
//				System.exit(-1); 
			} catch (IOException e) { 
				fileSave = false; 
				e.printStackTrace(); 
			} catch (InterruptedException e) { 
				e.printStackTrace(); 
			} finally{ 
				if(fileSave) 
					log.info(fpaths[0]+File.separator+key+" saved sucessfully"); 
				else 
					log.error("Failed to save "+ fpaths[0]+File.separator+key); 
			}
			Thread.sleep(450000l);
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
		//keywords.add("Account");keywords.add("Acid");keywords.add("Advance");keywords.add("Aerial");keywords.add("Agency");	keywords.add("Aggression");	keywords.add("Aggressor");keywords.add("Agitator");
		/*keywords.add("Aim");keywords.add("Aircraft");keywords.add("Airfield");keywords.add("Airplane");keywords.add("Airport");keywords.add("Alarm");keywords.add("Alert");	keywords.add("Alliance");keywords.add("Allies");
		keywords.add("Ambush");	keywords.add("Ammunition");	keywords.add("Anarchy");keywords.add("Anguish");keywords.add("Annihilate");	keywords.add("Apartheid");keywords.add("Appeasement");keywords.add("Armament");	keywords.add("Armed forces");
		keywords.add("Armory");	keywords.add("Arms");keywords.add("Arsenal");keywords.add("Artillery");keywords.add("Ashore");keywords.add("Assassin");keywords.add("Assassinate");	keywords.add("Assault");keywords.add("Atrocity");keywords.add("Attack");
		keywords.add("Attrition");keywords.add("Authority");keywords.add("Automatic");keywords.add("Barrage");keywords.add("Barricade");keywords.add("Battalion");keywords.add("Battle");keywords.add("Battlefield");keywords.add("Belligerent");
		keywords.add("Betray");keywords.add("Blast");keywords.add("Blindside");keywords.add("Blood");keywords.add("Bloody");keywords.add("Bloodletting");keywords.add("Blow up");keywords.add("Bomb");keywords.add("Bombardment");keywords.add("Booby trap");
		keywords.add("Breach");keywords.add("Brutal");keywords.add("Brutality");keywords.add("Bullet");keywords.add("Bulletproof");keywords.add("Bunker");keywords.add("Burn");keywords.add("Burning");keywords.add("Bury");keywords.add("Cadaver");
		keywords.add("Camouflage");keywords.add("Campaign");keywords.add("Captive");keywords.add("Captive");keywords.add("Capture");keywords.add("Careen");keywords.add("Cargo");keywords.add("Carnage");keywords.add("Carrier");keywords.add("Casualties");
		keywords.add("Cataclysm");keywords.add("Cautionary");keywords.add("Cautious");keywords.add("Chaos");keywords.add("Charge");keywords.add("Charred");keywords.add("Checkpoint");keywords.add("Chief");keywords.add("Chopper");keywords.add("Civilian");
		keywords.add("Clamor");keywords.add("Clandestine");keywords.add("Clash");keywords.add("Coalition");keywords.add("Collapse");keywords.add("Combat");keywords.add("Combat");keywords.add("Command(or)");keywords.add("Commandos");keywords.add("Compassion");
		keywords.add("Compliance");keywords.add("Concentration");keywords.add("Concussion");keywords.add("Conflagration");keywords.add("Conflict");keywords.add("Confrontation(al)");keywords.add("Confusion");keywords.add("Conquer");keywords.add("Consequences");
		keywords.add("Consolidate");keywords.add("Conspiracy");keywords.add("Conspire");keywords.add("Control");keywords.add("Convoy");keywords.add("Coordinate");keywords.add("Coordinates");keywords.add("Corps");keywords.add("Corpse");keywords.add("Counterattack");
		keywords.add("Countermand");keywords.add("Courageous");keywords.add("Crisis");keywords.add("Cross-hairs");keywords.add("Culpability");keywords.add("Damage");keywords.add("Danger");keywords.add("Dangerous");keywords.add("Dash");keywords.add("Dead");keywords.add("Deadly");
		keywords.add("Death");keywords.add("Debacle");keywords.add("Debris");keywords.add("Decline");keywords.add("Defect");keywords.add("Defend");keywords.add("Defense");keywords.add("Defensive");keywords.add("Defiant");keywords.add("Demolish");keywords.add("Demoralization");
		keywords.add("Deploy");keywords.add("Desert");keywords.add("Despot");keywords.add("Destroy");keywords.add("Destruction");keywords.add("Detect");keywords.add("Detection");keywords.add("Detonate");keywords.add("Devastation");keywords.add("Device");keywords.add("Dictator");
		keywords.add("Die");keywords.add("Disarmament");keywords.add("Disarray");keywords.add("Disaster");keywords.add("Disastrous");keywords.add("Discipline");keywords.add("Disease");keywords.add("Dismantle");keywords.add("Dispatch");keywords.add("Disperse");
		keywords.add("Dispute");keywords.add("Disruption");keywords.add("Dissonance");keywords.add("Dominate");keywords.add("Doom");keywords.add("Downfall");keywords.add("Drama");keywords.add("Dread");keywords.add("Drone");keywords.add("Duck");keywords.add("Duty");
		keywords.add("Encounter");keywords.add("Endurance");keywords.add("Enemy");keywords.add("Enforcement");keywords.add("Engagement");keywords.add("Epithet");keywords.add("Escalation");keywords.add("Escape");keywords.add("Evacuate");keywords.add("Evacuee");
		keywords.add("Excess");keywords.add("Execute");keywords.add("Execution");keywords.add("Exercise");keywords.add("Expectations");keywords.add("Explode");keywords.add("Exploitation");keywords.add("Explosion");keywords.add("Explosive");keywords.add("Expunge");
		keywords.add("Extremism");keywords.add("Faction");keywords.add("Fanatic");keywords.add("Fatal");keywords.add("Fear");keywords.add("Fearless");keywords.add("Ferment");keywords.add("Ferocious");keywords.add("Feud");keywords.add("Fierce");keywords.add("Fiery");
		keywords.add("Fight");keywords.add("Fighter");keywords.add("Flank");keywords.add("Flee");keywords.add("Flight");keywords.add("Forces");keywords.add("Forceful");keywords.add("Fortification");keywords.add("Foxhole");keywords.add("Fray");keywords.add("Frenzy");
		keywords.add("Fright");keywords.add("Front lines");keywords.add("Fuel");keywords.add("Fugitive");keywords.add("Furtive");keywords.add("Garrison");keywords.add("Gas");keywords.add("Generator");keywords.add("Genocide");keywords.add("Germ warfare");keywords.add("Government");
		keywords.add("Grave");keywords.add("Grenade");keywords.add("Grievous");keywords.add("Groans");keywords.add("Guard");keywords.add("Guerrillas");keywords.add("Guided bombs");keywords.add("Guns");keywords.add("Gunship");keywords.add("Hammering");keywords.add("Harsh");
		keywords.add("Hatch");keywords.add("Hate");keywords.add("Hatred");keywords.add("Hazard");keywords.add("Helicopter");keywords.add("Heroic");keywords.add("Heroism");keywords.add("Hide");keywords.add("Hijack");keywords.add("Hijacker");keywords.add("Hit");keywords.add("Hit-and-run");
		keywords.add("Holocaust");keywords.add("Horrific");keywords.add("Horses");keywords.add("Hostile");keywords.add("Hostility");keywords.add("Howitzer");keywords.add("Ignite");keywords.add("Impact");keywords.add("Improvise");keywords.add("Incident");keywords.add("Incite");
		keywords.add("Incontrovertible");keywords.add("Infantry");keywords.add("Inferred");keywords.add("Infiltrate");keywords.add("Inflame");keywords.add("Informant");keywords.add("Injuries");keywords.add("Instructions");keywords.add("Insurgent");keywords.add("Insurrection");
		keywords.add("Intelligence");keywords.add("Intense");keywords.add("Intercept");keywords.add("Interdiction");keywords.add("International");keywords.add("Interrogation");keywords.add("Intervene");keywords.add("Intimidate");keywords.add("Invasion");keywords.add("Investigate");
		keywords.add("Investigations");keywords.add("Involvement");keywords.add("Ire");keywords.add("Jeer");keywords.add("Jets");keywords.add("Join");keywords.add("Keening");keywords.add("Kidnap");keywords.add("Kill");keywords.add("Knives");keywords.add("Knock-out");keywords.add("Lamentation");
		keywords.add("Land mines");keywords.add("Laser-activated");keywords.add("Launch");keywords.add("Launcher");keywords.add("Legacy");keywords.add("Liaison");keywords.add("Liberation");keywords.add("Liberators");keywords.add("Links to");keywords.add("Loathsome");keywords.add("Loyalty");
		keywords.add("Machine guns");keywords.add("Machines");keywords.add("Maim");keywords.add("Malevolent");keywords.add("Malicious");keywords.add("Maraud");keywords.add("March");keywords.add("Massacre");keywords.add("Mayhem");keywords.add("Megalomania");keywords.add("Menace");keywords.add("Method");
		keywords.add("Militancy");keywords.add("Militant");keywords.add("Militaristic");keywords.add("Military");keywords.add("Militia");keywords.add("Mines");keywords.add("Missile");keywords.add("Mission");keywords.add("Mistreatment");keywords.add("Mobile");keywords.add("Mobilization");keywords.add("Momentum");
		keywords.add("Mortars");keywords.add("Munitions");keywords.add("Murder");keywords.add("Muscle");keywords.add("Nationalist");keywords.add("Negotiation");keywords.add("Neutralize");keywords.add("Nightmare");keywords.add("Nitrate");keywords.add("Notorious");keywords.add("Offensive");
		keywords.add("Officials");keywords.add("Onerous");keywords.add("Operation");keywords.add("Opposition");keywords.add("Order");keywords.add("Outbreak");keywords.add("Overrun");keywords.add("Overthrow");keywords.add("Pacify");keywords.add("Paramedics");keywords.add("Partisan");
		keywords.add("Patriot");keywords.add("Patriotism");keywords.add("Patrol");keywords.add("Penetration");keywords.add("Performance");keywords.add("Persecute");keywords.add("Petrify");keywords.add("Photos");keywords.add("Pilot");keywords.add("Pistol");keywords.add("Planes");keywords.add("Platoon");
		keywords.add("Plunder");keywords.add("Position");keywords.add("Post-traumatic");keywords.add("Potent");keywords.add("Pound");keywords.add("Powder");keywords.add("Power");keywords.add("Powerful");keywords.add("Preemptive");keywords.add("Premeditate");keywords.add("Prey");keywords.add("Prison");keywords.add("Prisoner");
		keywords.add("Proliferation");keywords.add("Provocation");keywords.add("Prowl");keywords.add("Pugnacious");keywords.add("Pulverize");keywords.add("Quail");keywords.add("Quarrel");keywords.add("Quell");keywords.add("Quiver");keywords.add("Radiation");keywords.add("Radical");
		keywords.add("Rage");keywords.add("Rally");keywords.add("Ravage");keywords.add("Ravish");keywords.add("Reaction");keywords.add("Readiness");keywords.add("Rebel");keywords.add("Rebellion");keywords.add("Reconnaissance");
		keywords.add("Recovery");keywords.add("Recruitment");keywords.add("Refugee");keywords.add("Regime");keywords.add("Regiment");keywords.add("Reinforcements");keywords.add("Relentless");keywords.add("Reparation");keywords.add("Reprisal");keywords.add("Reputation");keywords.add("Rescue");
		keywords.add("Resistance");keywords.add("Retaliation");keywords.add("Retreat");keywords.add("Retribution");keywords.add("Revenge");keywords.add("Revolution");keywords.add("Ricochet");keywords.add("Rifle");keywords.add("Rift");keywords.add("Rival");keywords.add("Rocket");keywords.add("Rot");keywords.add("Rounds");
		keywords.add("Rule");keywords.add("Ruthless");keywords.add("Sabotage");keywords.add("Sacrifice");keywords.add("Salvage");keywords.add("Sanction");keywords.add("Savage");keywords.add("Scare");keywords.add("Score");keywords.add("Scramble");keywords.add("Screening");keywords.add("Secrecy");keywords.add("Secret");
		keywords.add("Security");keywords.add("Sedition");keywords.add("Seize");keywords.add("Seizure");keywords.add("Sensor");keywords.add("Setback");keywords.add("Shelling");keywords.add("Shells");keywords.add("Shock");keywords.add("Shoot");keywords.add("Shot");keywords.add("Showdown");keywords.add("Siege");
		keywords.add("Skirmish");keywords.add("Slaughter");keywords.add("Smuggle");keywords.add("Soldier");keywords.add("Special-ops");keywords.add("Specialized");keywords.add("Spokesman");keywords.add("Spotter");keywords.add("Spy");keywords.add("Spy satellite");keywords.add("Squad");keywords.add("Stash");keywords.add("Stealth");
		keywords.add("Storage");keywords.add("Storm");keywords.add("Straggler");keywords.add("Strangle");keywords.add("Strategic");keywords.add("Strategist");keywords.add("Strategy");keywords.add("Strife");keywords.add("Strike");keywords.add("Strip");keywords.add("Stronghold");keywords.add("Struggle");keywords.add("Submarine");
		keywords.add("Subversive");keywords.add("Suffering");keywords.add("Superstition");keywords.add("Supplies");keywords.add("Support");keywords.add("Suppression");keywords.add("Surprise");keywords.add("Surrender");keywords.add("Survival");keywords.add("Survivor");keywords.add("Suspect");keywords.add("Tactics");keywords.add("Tank");
		keywords.add("Target");keywords.add("Tension");keywords.add("Terrain");keywords.add("Terror");keywords.add("Terrorism");keywords.add("Terrorist");keywords.add("Terrorize");keywords.add("Threat");keywords.add("Threaten");keywords.add("Thwart");keywords.add("Topple");keywords.add("Torch");keywords.add("Torpedo");
		keywords.add("Tourniquet");keywords.add("Tragic");keywords.add("Training");keywords.add("Trample");keywords.add("Transportation");keywords.add("Trap");keywords.add("Trauma");keywords.add("Treachery");keywords.add("Trench");keywords.add("Trigger");keywords.add("Triumph");keywords.add("Turbulent");keywords.add("Unbelievable");
		keywords.add("Unconventional");keywords.add("Unexpected");keywords.add("Uniform");keywords.add("Unify");keywords.add("Unit");keywords.add("Unite");keywords.add("Unleash");keywords.add("Uprising");keywords.add("Urgency");keywords.add("Vanguard");keywords.add("Vanish");keywords.add("Vehicle");keywords.add("Vehicular");
		keywords.add("Vendetta");keywords.add("Venomous");keywords.add("Veteran");keywords.add("Vicious");keywords.add("Victory");keywords.add("Vile");keywords.add("Vilify");keywords.add("Violation");keywords.add("Violence");keywords.add("Virulence");keywords.add("Vital");keywords.add("Vitriol");keywords.add("Vociferous");keywords.add("Void");
		keywords.add("Vow");keywords.add("Vulnerability");keywords.add("Wage");keywords.add("War");keywords.add("Warheads");keywords.add("Warnings");keywords.add("Warplane");keywords.add("Warrant");keywords.add("Warrior");keywords.add("Watch list");keywords.add("Watchdog");keywords.add("Watchful");keywords.add("Weapon");
		keywords.add("Well-trained");keywords.add("Whiz");keywords.add("Wince");keywords.add("Wisdom");keywords.add("Worldwide");keywords.add("Wounds");keywords.add("Wreckage");keywords.add("X-ray");keywords.add("Yearn");keywords.add("Yelling");keywords.add("Zeal");keywords.add("Zealot");keywords.add("Zigzag");keywords.add("Zone");*/
		
		/*keywords.add("Al Badr");keywords.add("Hizbul-Mujahideen");keywords.add("Lashkar-e-Taiba");keywords.add("Jaish-e-Mohammad");keywords.add("Harkat-e-Mujahideen");keywords.add("United Liberation Front of Asom");
		keywords.add("ULFA");keywords.add("National Democratic Front of Bodoland");keywords.add("NDFB");keywords.add("United People's Democratic Solidarity");keywords.add("UPDS");keywords.add("Kamtapur Liberation Organisation");
		keywords.add("KLO");keywords.add("Bodo Liberation Tiger Force");keywords.add("BLTF");keywords.add("Dima Halim Daogah");keywords.add("DHD");keywords.add("Karbi National Volunteers");keywords.add("KNV");keywords.add("Rabha National Security Force");
		keywords.add("RNSF");keywords.add("Koch-Rajbongshi Liberation Organisation");keywords.add("KRLO");keywords.add("Hmar People's Convention- Democracy");keywords.add("HPC-D");keywords.add("Karbi People's Front");keywords.add("KPF");
		keywords.add("Tiwa National Revolutionary Force");keywords.add("TNRF");keywords.add("Bircha Commando Force");keywords.add("BCF");keywords.add("Bengali Tiger Force");keywords.add("BTF");keywords.add("Adivasi Security Force");
		keywords.add("All Assam Adivasi Suraksha Samiti");keywords.add("Gorkha Tiger Force");keywords.add("Barak Valley Youth Liberation Front");
		keywords.add("Muslim United Liberation Tigers of Assam");keywords.add("MULTA");keywords.add("United Liberation Front of Barak Valley");keywords.add("Muslim United Liberation Front of Assam");keywords.add("MULFA");keywords.add("Muslim Security Council of Assam");
		keywords.add("United Liberation Militia of Assam");keywords.add("ULMA");keywords.add("Islamic Liberation Army of Assam");keywords.add("ILAA");keywords.add("Muslim Volunteer Force");keywords.add("Muslim Liberation Army");
		keywords.add("Muslim Security Force"); 	*/keywords.add("Islamic Sevak Sangh");keywords.add("Islamic United Reformation Protest of India");keywords.add("United Muslim Liberation Front of Assam");
		keywords.add("Revolutionary Muslim Commandos");keywords.add("Muslim Tiger Force");keywords.add("People's United Liberation Front");keywords.add("Adam Sena");
		keywords.add("Harkat-ul-Mujahideen");keywords.add("Harkat-ul-Jehad");keywords.add("Lashkar-e-Omar");keywords.add("Hizb-ul-Mujahideen");keywords.add("Harkat-ul-Ansar");keywords.add("HuA, presently known as Harkat-ul Mujahideen");
		keywords.add("Lashkar-e-Toiba");keywords.add("Jaish-e-Mohammed");keywords.add("Harkat-ul Mujahideen");keywords.add("HuM, previously known as Harkat-ul-Ansar");keywords.add("Jamait-ul-Mujahideen");
		keywords.add("Lashkar-e-Jabbar");keywords.add("Harkat-ul-Jehad-i-Islami");keywords.add("Al Barq");keywords.add("Tehrik-ul-Mujahideen");keywords.add("Al Jehad");keywords.add("Jammu & Kashir National Liberation Army");
		keywords.add("People's League");keywords.add("Muslim Janbaz Force");keywords.add("Kashmir Jehad Force");keywords.add("Al Jehad Force");keywords.add("combines Muslim Janbaz Force and Kashmir Jehad Force");keywords.add("Al Umar Mujahideen");keywords.add("Mahaz-e-Azadi");
		keywords.add("Islami Jamaat-e-Tulba");keywords.add("Jammu & Kashmir Students Liberation Front");keywords.add("Ikhwan-ul-Mujahideen");keywords.add("Islamic Students League");keywords.add("Tehrik-e-Hurriat-e-Kashmir");keywords.add("Tehrik-e-Nifaz-e-Fiqar Jafaria");
		keywords.add("Al Mustafa Liberation Fighters");keywords.add("Tehrik-e-Jehad-e-Islami");keywords.add("Muslim Mujahideen");keywords.add("Al Mujahid Force");keywords.add("Tehrik-e-Jehad");keywords.add("Islami Inquilabi Mahaz");keywords.add("Mutahida Jehad Council");
		keywords.add("Jammu & Kashmir Liberation Front");keywords.add("All Parties Hurriyat Conference");keywords.add("Dukhtaran-e-Millat");keywords.add("United National Liberation Front");
		keywords.add("People's Liberation Army");keywords.add("People's Revolutionary Party of Kangleipak");keywords.add("The above mentioned three groups now operate from a unified platform, the Manipur People's Liberation Front");
		keywords.add("Kangleipak Communist Party");keywords.add("Kanglei Yawol Kanna Lup");keywords.add("Manipur Liberation Tiger Army");keywords.add("Iripak Kanba Lup");keywords.add("People's Republican Army");
		keywords.add("Kangleipak Kanba Kanglup");keywords.add("Kangleipak Liberation Organisation");keywords.add("Revolutionary Joint Committee");keywords.add("National Socialist Council of Nagaland -- Isak-Muivah");
		keywords.add("People's United Liberation Front");keywords.add("North East Minority Front");keywords.add("Islamic National Front");keywords.add("Islamic Revolutionary Front");keywords.add("United Islamic Liberation Army");
		keywords.add("United Islamic Revolutionary Army");keywords.add("Kuki National Front");keywords.add("Kuki National Army");keywords.add("Kuki Revolutionary Army");keywords.add("Kuki National Organisation");
		keywords.add("Kuki Independent Army");keywords.add("Kuki Defence Force");keywords.add("Kuki International Force");keywords.add("Kuki National Volunteers");keywords.add("Kuki Liberation Front");keywords.add("Kuki Security Force");
		keywords.add("Kuki Liberation Army");keywords.add("Kuki Revolutionary Front");keywords.add("United Kuki Liberation Front");keywords.add("Hmar People's Convention");keywords.add("Hmar People's Convention- Democracy");
		keywords.add("Hmar Revolutionary Front");keywords.add("Zomi Revolutionary Army");keywords.add("Zomi Revolutionary Volunteers");keywords.add("Indigenous People's Revolutionary Alliance(IRPA");keywords.add("Kom Rem People's Convention");
		keywords.add("Chin Kuki Revolutionary Front");keywords.add("Hynniewtrep National Liberation Council");keywords.add("Achik National Volunteer Council");keywords.add("People's Liberation Front of Meghalaya");keywords.add("Hajong United Liberation Army");
		keywords.add("National Socialist Council of Nagaland");keywords.add("Isak-Muivah NSCN(IM)");keywords.add("National Socialist Council of Nagaland");keywords.add("Khaplang NSCN");keywords.add("Naga National Council");keywords.add("Adino NNC");keywords.add("Babbar Khalsa International");
		keywords.add("Khalistan Zindabad Force");keywords.add("International Sikh Youth Federation");keywords.add("Khalistan Commando Force");keywords.add("All-India Sikh Students Federation");keywords.add("Bhindrawala Tigers Force of Khalistan");
		keywords.add("Khalistan Liberation Army");keywords.add("Khalistan Liberation Front");keywords.add("Khalistan Armed Force");keywords.add("Dashmesh Regiment");keywords.add("Khalistan Liberation Organisation");keywords.add("Khalistan National Army");
		keywords.add("National Liberation Front of Tripura");keywords.add("All Tripura Tiger Force");keywords.add("Tripura Liberation Organisation Front");keywords.add("United Bengali Liberation Front");keywords.add("Tripura Tribal Volunteer Force");
		keywords.add("Tripura Armed Tribal Commando Force");keywords.add("Tripura Tribal Democratic Force");keywords.add("Tripura Tribal Youth Force");keywords.add("Tripura Liberation Force");keywords.add("Tripura Defence Force");
		keywords.add("All Tripura Volunteer Force");keywords.add("Tribal Commando Force");keywords.add("Tripura Tribal Youth Force");keywords.add("All Tripura Bharat Suraksha Force");keywords.add("Tripura Tribal Action Committee Force");
		keywords.add("Socialist Democratic Front of Tripura");keywords.add("All Tripura National Force");keywords.add("Tripura Tribal Sengkrak Force");keywords.add("Tiger Commando Force");keywords.add("Tripura Mukti Police");keywords.add("Tripura Rajya Raksha Bahini");
		keywords.add("Tripura State Volunteers");keywords.add("Tripura National Democratic Tribal Force");keywords.add("National Militia of Tripura");keywords.add("NMT");keywords.add("All Tripura Bengali Regiment");keywords.add("ATBR");keywords.add("Bangla Mukti Sena");keywords.add("BMS");
		keywords.add("All Tripura Liberation Organisation");keywords.add("Tripura National Army");keywords.add("Tripura State Volunteers");keywords.add("Borok National Council of Tripura");keywords.add("Bru National Liberation Front");keywords.add("Hmar People's Convention- Democracy");
		keywords.add("HPC-D");keywords.add("Arunachal Pradesh ");keywords.add("Arunachal Dragon Force");keywords.add("Communist Party of India-Maoist");keywords.add("CPI-Maoist");keywords.add("People's War Group");keywords.add("Maoist Communist Centre");keywords.add("People's Guerrilla Army");keywords.add("Communist Party of India");keywords.add("Marxist Leninist) Janashakti");
		keywords.add("Tritiya Prastuti Committee");keywords.add("Tamil National Retrieval Troops");keywords.add("Akhil Bharat Nepali Ekta Samaj");keywords.add("Tamil Nadu Liberation Army");keywords.add("Deendar Anjuman");keywords.add("Students Islamic Movement of India");keywords.add("SIMI");
		keywords.add("Asif Reza Commando Force");keywords.add("Liberation Tigers of Tamil Eelam");keywords.add("LTTE");keywords.add("Kamatapur Liberation Organisation");keywords.add("Ranvir Sena");keywords.add("National Technical Research Organisation");keywords.add("NTRO");keywords.add("Research and Analysis Wing");keywords.add("R&AW or RAW");
		keywords.add("Intelligence Bureau (IB)");keywords.add("Narcotics Control Bureau (NCB)");keywords.add("Defence Intelligence Agency");keywords.add("Defence Information Warfare Agency (DIWA)");keywords.add("Defence Image Processing and Analysis Centre"); keywords.add("DIPAC");keywords.add("Joint Cipher Bureau");keywords.add("Signals Intelligence Directorate");
		keywords.add("Directorate of Air Intelligence");keywords.add("Directorate of Navy Intelligence");keywords.add("IURPI");keywords.add("LeT");keywords.add("JeM");keywords.add("JuM");keywords.add("LeJ");keywords.add("JKLF");keywords.add("DeM");
		keywords.add("PREPAK");keywords.add("UILA");keywords.add("UIRA");keywords.add("HPC-D");keywords.add("NSCN-IM");keywords.add("UMLFA");keywords.add("PLF-M");keywords.add("Adino");
		Calendar calendar = Calendar.getInstance(); 
		calendar.set(Calendar.YEAR, 2014); 
		calendar.set(Calendar.MONTH, 9); 
		calendar.set(Calendar.DAY_OF_MONTH, 26); // new years eve 
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 
		String[] fpaths = {"/user/dev11/keywords17"};
		try {
			TwitterUtil.searchTweets(keywords, format.format(calendar.getTime()), fpaths);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	} 
}
