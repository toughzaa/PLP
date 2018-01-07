package cs.bigdata.Tutorial2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class PageRankMapper extends Mapper<IntWritable, Text, Text, Text> {

	//private Text word = new Text();

	protected void map(Text keyE, IntWritable valE, Context context) throws IOException,InterruptedException
    {
		String line = keyE.toString();
		
		char firstLetter = line.charAt(8);
		
		// skip the first lines
		if(firstLetter != '#') {
			
			String[] content = line.split(" ");
			
			// source
			Text keyS = new Text(content[0]);
			
			// target
			Text valS = new Text(content[1]);

			context.write(keyS, valS);
		}
    }
}
