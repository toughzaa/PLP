package cs.bigdata.Lab2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
//import java.io.Iterable;
import java.util.Iterator;
import java.util.Map;

// To complete according to your problem

public class WordCountsPerDocReducer extends Reducer<Text, Text, Text, Text> {

    // Overriding of the reduce function
	private IntWritable count = new IntWritable();

    protected void reduce(Text cleI, Iterable<Text> listevalI, Context context) throws IOException,InterruptedException
    {
    	
    	int wordsInDoc = 0;

    	Map<String, Integer> tempCounter = new HashMap<String, Integer>();
    	
        for (Text val: listevalI) {
        	
        	String content = val.toString();
        	String[] array = content.split("$");
        	
        	// Retrieve the word and its wordcount
    		Text word = new Text(array[0]);
    		Integer count = Integer.valueOf(array[1]);
    		//IntWritable wordCount = new IntWritable(count);
    		
    		// Add the key to the temp table
    		tempCounter.put(array[0], count);
    		
    		// Compute total number of words per doc
    		wordsInDoc += count;
        }
        
        for(Map.Entry<String, Integer> entry : tempCounter.entrySet()) {
            String keyWord = entry.getKey();
            Integer wdCount = entry.getValue();
            
            String cle = cleI.toString();
            cle = keyWord + "$" + cle;
            Text cleS = new Text(cle);
            
            String value = Integer.toString(wdCount) + "$" + Integer.toString(wordsInDoc);
            Text valueS = new Text(value);

            context.write(cleS, valueS);
        }
     
    }
}
