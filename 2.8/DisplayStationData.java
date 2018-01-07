package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class DisplayStationData {
	public static void main(String[] args) throws IOException {
		
		String localSrc = "C:\\Users\\Adam\\Downloads\\isd-history.txt";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// skip the first 22 lines
			for(int i=1;i<=22;i++)
			{
			    br.readLine();
			}
			
			// read line by line
			String line = br.readLine();
			
			String stationUSAF, stationName, stationFIPS, stationElevation;
			
			while (line != null){
				
				// extract data
				stationUSAF = line.substring(0, 6);
				stationName = line.substring(12, 42);
				stationFIPS = line.substring(42, 45);
				stationElevation = line.substring(73, 81);
				
				// Print data
				System.out.println("Station : " + stationUSAF + " - Name : " + stationName + " - Country : " + stationFIPS + " - Elevation :" + stationElevation);
				
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
	}

}
