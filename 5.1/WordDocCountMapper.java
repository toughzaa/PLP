package cs.bigdata.Lab2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordDocCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();

	protected void map(Text keyE, IntWritable valE, Context context) throws IOException,InterruptedException
    {
		
		String keyLine = keyE.toString();
		String[] keyArray = keyLine.split("$");
		
		Text key = new Text(keyArray[0]);
		String docID = keyArray[1];
		
		String valLine = valE.toString();
		valLine = docID  + valLine;
		
		Text valS = new Text(valLine);
		
		context.write(key, valS);
    }
}
