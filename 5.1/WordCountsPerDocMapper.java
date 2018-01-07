package cs.bigdata.Lab2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordCountsPerDocMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();

	protected void map(Text keyE, IntWritable valE, Context context) throws IOException,InterruptedException
    {
		
		String line = keyE.toString();
		String[] array = line.split("$");
		
		Text docID = new Text(array[1]);
		
		String val = valE.toString();
		String key = array[0];
		val = key + "$" + val;
				
		Text valS = new Text(val);
		
		context.write(docID, valS);
    }
}
