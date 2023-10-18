import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class MS_crawler{
	
		public static HashMap<String, String> WordMap = new HashMap<String, String>();
		public static HashMap<String, String> DiseaseBank = new HashMap<String, String>();
		public static int page_count;
		public static int review_count;
		public static int count ;
		public static long Byte_sum;
		public static BufferedReader brReader;
		public static String path  = new File("").getAbsolutePath();  
		public static String log_path;
		public static String ClientName;   
		public static String Department;
		public static String competition;
		public static int is_comp;
		public static int client_id;
		public static String str_client_id ;
		public static String str_jobID ;
		public static int jobID ;
		public static int source_id = 1;
		
		/*DATABASE*/
		public static String jdbcDriver = "com.mysql.jdbc.Driver";
	    public static String db_url = "jdbc:mysql://localhost:3306/d2rm1.1?autoReconnect=true&useSSL=false";
	    public static String userPass = "?user=root&password=";
	    public static String dbName = "d2rm1.1";
	    public static String username = "root";
	    public static String password = "Alethe@123";
	    public static PreparedStatement preStatement;
	    public static Statement statement;
	    public static Statement st;
	    public static ResultSet rs;
	    public static Connection conn;
	    public static String query;
	    
	    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	    public static DateFormat tf = new SimpleDateFormat("HHmmss");
	    public static Calendar cal = Calendar.getInstance();
	    public static Date system_date_java = new Date();					
		public static java.sql.Timestamp system_date_sql = new java.sql.Timestamp(system_date_java.getTime());;
		public static  java.sql.Date latest_date;
		public static java.sql.Timestamp oldest_date ;
		public static Date start = new Date();
		public static java.sql.Timestamp endTime;
		public static java.sql.Timestamp Start = new java.sql.Timestamp(start.getTime());
		public static java.sql.Date sqlDate;
		public static String page_id ;
		public static boolean flag = false;
	    
	    
	    
	    public static void main(String[] args) throws IOException{
	    	
	    	if (args.length > 0) {
	    	    try {
	    	    	str_client_id = args[0];
	    	    	client_id = Integer.parseInt(str_client_id);
	    	    	str_jobID = args[1];
	    	    	jobID = Integer.parseInt(str_jobID);
	    	    } catch (NumberFormatException e) {
	    	        log_printer(path , "Argument" + args[0] + " must be an integer.");
	    	        System.exit(1);
	    	        
	    	    }
	    	}
	    	try{
	    		is_comp = 0;
	    		count = 0;
				review_count =0;
				Byte_sum = 0;
				Date start = new Date();
		    	Start = new java.sql.Timestamp(start.getTime());   //to conver sql date time
	    		
	    	log_printer(path , "Mouthshut Crawler Started");
	    	/*Database connection*/
	    	log_printer(path, "Connecting to database - JDBC URL:"+db_url+"; User Name:" + username +"; Passowrd:" + password);  //System.out.println("Connecting database...");
	    	conn = DriverManager.getConnection(db_url, username, password);
	    	log_printer(path, "Database Connected!!");
	  

	    	query = "update crawling_scheduler set is_scheduled = 1 where scheduler_id = "+jobID+";";
	      	st = conn.createStatement();
			st.executeUpdate(query);
	    	
	    	query = "SELECT * FROM crawling_scheduler where scheduler_id = "+jobID+";";
	    	st = conn.createStatement();
	    	rs = st.executeQuery(query);
	    	String url;
	    	List<String>  MSlinks = new ArrayList<String>();
	    	List<String> ClientList = new ArrayList<String>();
	    	while(rs.next())
	    	{	
	    		url = rs.getString("url");
	    		competition = rs.getString("competition");
	    		is_comp = rs.getInt("competition_flag");
	    		ClientList.add(competition);
	    		MSlinks.add(url);
	    	}
	    	log_printer(path , "Number of link to be crawled: "+"\t" + MSlinks.size());
	    	
	    	query = "SELECT client_name from clients where client_id="+client_id+";";
	    	st = conn.createStatement();
	    	rs = st.executeQuery(query);
	    	rs.next();
	    	//String client = rs.getString("client_name");
	    	rs.close();
	    	
	    	MouthShut_crawler(MSlinks, ClientList);
	    	
	    	Date dt = new Date();
	    	endTime =  new java.sql.Timestamp(dt.getTime());
	    	query = "update crawling_scheduler set is_scheduled = 0, last_crawled_date = '" + endTime + "' where scheduler_id = "+jobID+";";
	    	st = conn.createStatement();
			st.executeUpdate(query);
			
			Update_scheduler_logs("Successful", review_count, Byte_sum);
			conn.close();
			log_printer(path , "Number of reviews inserted into database: "+"\t" + review_count);
			log_printer(path , "Total Bytes inserted into database: "+"\t" + Byte_sum);
			
	    }catch(Exception e){
	    	log_printer(path , e.getMessage());
	    	Update_scheduler_logs("Interrupted", review_count, Byte_sum);
	    	
	    }
	    	
	    }
	  
	    public static void MouthShut_crawler(List<String> MSlinks, List<String> ClientList) throws IOException{
			int numOfClients = ClientList.size();
			try{
			for(int Cnum = 0; Cnum < numOfClients; Cnum++)
			{
				//competition = CompetitionList[Cnum];
				ClientName = ClientList.get(Cnum);
				log_printer(path , "Client Name:"+"\t" + ClientName);
				String mouthshut_link = MSlinks.get(Cnum);   //example ===>> "http://www.mouthshut.com/product-reviews/Croma-Mumbai-reviews-925076645";
				page_id = mouthshut_link;
				
				query = "SELECT  max(date), min(date) FROM reviews WHERE  client_id = '"+client_id+"' and source_id = 1 and page_id = '"+page_id+"';";
				st = conn.createStatement();
				rs = st.executeQuery(query);
				rs.next();
				latest_date = rs.getDate(1);
				oldest_date = rs.getTimestamp(2);
				if(latest_date == null){latest_date = new java.sql.Date(0);}
				if(oldest_date == null){oldest_date = system_date_sql;}
				
				
				if(mouthshut_link.equals("NA")) { log_printer(path , "No Related Data Available on Mouthshut.com");}
				else{
					System.out.println(mouthshut_link);
					Document doc1 = Jsoup.connect(mouthshut_link).timeout(1000*1000).userAgent("Chrome/26.0.1410.64").get();
					//System.out.println(doc1.toString());
					Elements MouthshutPageLinks = doc1.select("div[class*= reviews]");
					String[] total_reviews_string = MouthshutPageLinks.first().getElementsByTag("a").first().ownText().split(" ");
					int total_reviews = Integer.parseInt(total_reviews_string[0]);
					int pages = 1;
					if(total_reviews % 20 == 0){pages =  total_reviews/20;} 
					else { pages = total_reviews/20 + 1;}// one page contains 20 reviews
					log_printer(path, "Number of pages available on mouthshut = "+pages);
					for(int pageCount = 1 ; pageCount <= pages; pageCount++)
					{	
						try{
						Document doc11 = Jsoup.connect(mouthshut_link + "-page-" + pageCount).timeout(1000*1000).userAgent("Chrome/26.0.1410.64").get();
						int bytes = doc11.toString().length();
						
						log_printer(path, "Link being crawled:"+"\t"+mouthshut_link + "-page-" + pageCount);
						getMouthShutReviews(doc11, ClientName);
						log_printer(path, "Crawled date size:"+"\t"+ bytes);
						if(flag==true){flag = false; break;}
						
						}catch(Exception e){log_printer(path, e.getMessage());}
					}
				}
			}
			}catch (Exception e){log_printer(path, e.getMessage());}
			}
	    
	    
	    public static void getMouthShutReviews(Document doc, String ClientName) throws IOException{
			
			Element ul = doc.getElementById("dvreview-listing");
			//log_printer(path,"New Page \n" + doc.toString());
			int user_review_count = 0;
			for(int count = 0; count < 20; count++)
			{
				
				try{
					/*User Profile information*/
					Element li = ul.select("div[class*=row review-article]").get(count);
					Element row = li.select("div[class*= row]").first();
				/*
					Element prfl = doc.select("div[class*=col-2 profile]").first();
					Elements prof_str = prfl.getElementsByTag("script");
					for (Element element :prof_str ){                
				        for (DataNode node : element.dataNodes()) {
				            System.out.println(node.toString());
				        }
				        System.out.println("-------------------");            
				  }
					
					//System.out.println("User : " + prof_str.val(JSONML.stringify(line)));
					
					Element profile = li.select("div[class*= col-10 review]").first();
				*/
					String user = "Anonymous";
//					String geolocation = profile.getElementsByTag("p").get(1).ownText();
					String geolocation = "India";
//					String[] reviews_str = profile.getElementsByTag("a").get(2).ownText().split(" ");
//					user_review_count = Integer.parseInt(reviews_str[0]);
					user_review_count = 5;
					Element link_element = li.select("div[class*=col-10 review]").first();

					String link_str =  link_element.getElementsByTag("a").get(0).attr("href");
			
					/*Review Content*/
					Element rating = li.select("p[class*=rating]").first();
//					int ratingStar = rating.select("i[class*=icon-rating rated-star]").size();
					int ratingStar = 5;					
					String date_str = rating.getElementsByTag("span").get(1).ownText();

					String review_text = li.select("div[class*=more reviewdata]").get(0).ownText();//("more reviewdata").get(1).ownText();//select("div[class* = more reviewdata]").first();
				    //System.out.println(review_text + "\n\n\n");					
					/*This is code is written for converting date string into java date object*/
					Date timePublished = null;
					java.sql.Time sqlTime = null;
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
					Calendar cal = Calendar.getInstance();
				
					/*Following line of code detect if the review date is present or not. In case 'days ago' is present in case of date we will check
					the days and convert the it into date.*/
					if(date_str.length() == 0){sqlDate = null;sqlTime = null;}
					else if(date_str.indexOf("ago") > 0)
						{
							String datePub[] = date_str.split(" ");
							cal = Calendar.getInstance();
							cal.add(Calendar.DATE, -1*Integer.parseInt(datePub[0]));		
							sqlDate = new java.sql.Date(cal.getTime().getTime());
							sqlTime = new java.sql.Time(cal.getTime().getTime());
						}
						else
						{							
							dateFormat = new SimpleDateFormat("MMM dd, yyyy hh:mm a");
						    Date datePublished = dateFormat.parse(date_str);
							sqlDate = new java.sql.Date(datePublished.getTime()); 
							String timeString = date_str.substring(13);          
							dateFormat = new SimpleDateFormat("hh:mm a");			 // to extract "hh:mm a" from "MMM dd, yyyy hh:mm a"
							dateFormat.setLenient(true);
							timePublished = dateFormat.parse(timeString);
							sqlTime = new java.sql.Time(timePublished.getTime());
						}
					if(latest_date.before(sqlDate) || oldest_date.after(sqlDate) ){
						
					
						 if(latest_date.toString().equals(sqlDate.toString())){
							query = "select link from reviews where client_id = "+client_id+" and source_id = "+source_id+ " and date = '" + sqlDate + "' and link = '"+link_str+"' ;";
							//System.out.println(query);
							st = conn.createStatement();
							rs = st.executeQuery(query);
							if(!rs.next())
							{	
								insert_funct(link_str, sqlDate, sqlTime, review_text, user_review_count, ratingStar);							}
							else{flag = true; break;}
						 }
						 else
						 {
								 insert_funct(link_str, sqlDate, sqlTime, review_text, user_review_count, ratingStar);	
						}
		
					}
					else{flag = true; break;}
					
			    }catch(Exception ex) {log_printer(path, ex.getMessage());}
				
			}
		
		}
	    
	   
		public static void insert_funct(String link_str, java.sql.Date sqlDate, java.sql.Time sqlTime, String review_text, int user_review_count, int ratingStar) throws IOException
	    {
	    	try{
		     query = "INSERT INTO reviews(client_id, date, time, competition, source_id, link, review_text, geolocation, posted_by, data_type, language, is_deleted, insert_date, page_id, is_competition) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?);"; 
				    
		     preStatement = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
		     preStatement.setInt(1, client_id);
		     preStatement.setDate(2, sqlDate);
		     preStatement.setTime(3, sqlTime);
		     preStatement.setString(4, ClientName);
		     preStatement.setInt(5, source_id);
		     preStatement.setString(6, link_str);
		     preStatement.setString(7, review_text);
		     preStatement.setString(8, "NA");   				//To be implemented
		     preStatement.setString(9, "NA");					//To be implemented
		     preStatement.setString(10, "Text");				//To be implemented
		     preStatement.setString(11, "English");				//To be implemented
		     preStatement.setInt(12, 0);
		     preStatement.setTimestamp(13, system_date_sql);
		     preStatement.setString(14, page_id);
		     preStatement.setInt(15, is_comp);
		     preStatement.execute();
		     ResultSet rs = preStatement.getGeneratedKeys();
		     int generatedKey = 0;
		     if (rs.next()) {
		     generatedKey = rs.getInt(1);
		    	}
		     
		     log_printer(path,"Inserted record's ID: " + generatedKey);
		     review_count++;
		     
		     query = "INSERT INTO reviews_extended(id, source_id, client_id, tweets, rating) VALUES (?, ?, ?, ?, ?);"; 
		    
		     preStatement = conn.prepareStatement(query);
		     preStatement.setInt(1,  generatedKey);
		     preStatement.setInt(2, source_id);
		     preStatement.setInt(3, client_id);
		     //preStatement.setInt(3, shared);
		     //preStatement.setInt(4, likes);
		    // preStatement.setInt(4, followers);
		     //preStatement.setInt(6, following);
		     preStatement.setInt(4, user_review_count);
		     preStatement.setInt(5, ratingStar);
		     preStatement.execute();
			
			 }catch(Exception e ){log_printer(path, e.getMessage());}
	    }

	    
	    
	    public static void Update_scheduler_logs(String status, int review_count, long Byte_sum) throws IOException
	    {
	    	
	    	try{
	    		Date dt = new Date();
		    	endTime =  new java.sql.Timestamp(dt.getTime());
	    		query = "select scheduler_id from scheduler_logs where scheduler_id = "+jobID;
	    		st = conn.createStatement();
				ResultSet rSet = st.executeQuery(query);
				if(rSet.next())
				{
					query = "update scheduler_logs set scheduler_start = '"+ Start+ "',  scheduler_end = '"+ endTime+"', review_count = "+review_count+",  scheduler_status = '"+status+"' , data_size = "+ Byte_sum +" where scheduler_id = "+jobID+ ";";
					st = conn.createStatement();
					st.executeUpdate(query);
					rSet.close();
				}
				
				else{
					
					query = "INSERT INTO scheduler_logs(scheduler_id, client_id, scheduler_start, scheduler_end, review_count, data_size, scheduler_status) VALUES (?, ?, ?, ?, ?, ?, ?);"; 
					PreparedStatement preStatement = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
					preStatement.setInt(1, jobID);
					preStatement.setInt(2, client_id);
					preStatement.setTimestamp(3, Start );
					preStatement.setTimestamp(4, endTime);
					preStatement.setInt(5, review_count);
					preStatement.setLong(6, Byte_sum);
					preStatement.setString(7, status);
					preStatement.execute();
				}
	    		}catch(Exception e){log_printer(path, e.getMessage());}
	    }
	    


	    

		public static void log_printer(String path, String message) throws IOException
	    {
	    	String sfileName = str_jobID+"__"+ tf.format(system_date_java)+"_MouthShut.txt";
	    	String str_date = df.format(system_date_java);
	    	new File(path + "/Crawler_log_files/" + str_date+"/"+str_client_id).mkdirs();
	    	log_path = path + "/Crawler_log_files/" + str_date+"/"+str_client_id;
	    	File myFile = new File(log_path + "/" +sfileName);
			//myFile.createNewFile();		// if file already exists will do nothing 
			FileWriter fw = new FileWriter(myFile.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			SimpleDateFormat tmsf = new SimpleDateFormat("HH:mm:ss.SSS");
			Date curr_date = new Date();
			bw.write(tmsf.format(curr_date)+"\t"+ message);
			bw.newLine();
			bw.close();	
	    }
	    
	    
	    
}