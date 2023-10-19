import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class CPCB_crawler {

	private static String link = "https://api.openaq.org/v1/measurements";
	private static String date, country, unit, city,  parameter, station, state;
	private static double latitude, longitude, value;
	private static int location_id;
	static Connection conn = null;
	static String path = new File("").getAbsolutePath();

	public static void main(String[] args) throws IOException {

		//Document doc1 = Jsoup.connect(link).get();
		
		conn = dbConnect();
		Statement stmt;
		ResultSet rs;
		String query = "Select * from location;";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				location_id = rs.getInt("id");
				country = rs.getString("country");
				state = rs.getString("state");
				city = rs.getString("city");
				station = rs.getString("station");
				
				Document doc = null;
				try {
					doc = Jsoup.connect(link+"?location="+station).ignoreContentType(true).timeout(10*1000).get();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String json_string = doc.getElementsByTag("body").first().text();
				JSONObject js = new JSONObject(json_string);
				JSONArray result = (JSONArray) js.get("results");
				int temp_count = 0;
				for(int i=0; i< result.length();i++) {
					JSONObject dataset = result.getJSONObject(i);
					
					parameter = dataset.getString("parameter");
					date = ((JSONObject)dataset.get("date")).get("local").toString();
					value = dataset.getDouble("value");
					unit = dataset.getString("unit");
					latitude = ((JSONObject)dataset.get("coordinates")).getDouble("latitude");
					longitude = ((JSONObject)dataset.get("coordinates")).getDouble("longitude");
					
					query = "INSERT INTO polution_data_temp (location_id, parameter, value, unit, data_datetime, insert_datetime) VALUES ('" + location_id + "', '"+parameter+"', '" + value + "', '" + unit + "', '" + date + "', '" + new Timestamp(new Date().getTime()) + "');";
					rs = stmt.executeQuery(query);
					
					if(temp_count == 0) {
						String query2 = "INSERT INTO location (latitude,longitude) VALUES ('"+latitude+"','"+longitude+"') WHERE station = '"+station+"';";
						rs = stmt.executeQuery(query2);
						temp_count++;
					}
				}
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static Connection dbConnect() throws IOException {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://192.168.1.160:3306/capperdb", "root", "Alethe@123");
			System.out.println("Successfully Connected to database.");
			return con;
		}
		catch(Exception e)
		{
			System.out.println("error is "+e.getMessage());
			return null;
		}
	}

}
