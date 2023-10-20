import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class crawlogger {
	static String path  = new File("").getAbsolutePath();
	static Date system_date_java = new Date();
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	static DateFormat tf = new SimpleDateFormat("HHmmss");
	
	public void logger(String fid, String  fileName, String message) throws IOException
	{
		String strTime = tf.format(system_date_java);
		String sfileName =  fileName + "_"+ strTime + ".txt";
		String str_date = df.format(system_date_java);
		new File(path + "\\Crawlers log\\" + str_date+"\\"+ fid).mkdirs();
		String log_path = path + "\\Crawlers log\\" + str_date+"\\"+ fid;		
		File myFile = new File(log_path + "\\" + sfileName);
		myFile.createNewFile();		// if file already exists will do nothing 
		FileWriter fw = new FileWriter(myFile.getAbsoluteFile(),true);
		BufferedWriter bw = new BufferedWriter(fw);
		SimpleDateFormat tmsf = new SimpleDateFormat("HH:mm:ss.SSS");
		Date curr_date = new Date();
		bw.write(tmsf.format(curr_date)+"\t"+ message);
		bw.newLine();
		bw.close();	
	}
}
