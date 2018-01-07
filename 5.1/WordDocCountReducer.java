package cs.bigdata.Lab2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
//import java.io.Iterable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import java.lang.*;

// To complete according to your problem

public class WordDocCountReducer extends Reducer<Text, Text, Text, Text> {

    // Overriding of the reduce function
	private IntWritable count = new IntWritable();

    protected void reduce(Text cleI, Iterable<Text> listevalI, Context context) throws IOException,InterruptedException
    {
    	
     	//  Total number of docs
     	int numberOfDocs = 4;
     	
     	int DocForWord = 0;
     		// Nombre total d'occurences du mot et comptabilisation du nombre de documents avec le mot en question 
    		
   		Map<String, String> temp = new HashMap<String, String>(); 
    		
   		for (Text val : listevalI) {
   			String content = val.toString();
           	String[] array = content.split("$");
           	
        	// Add the key to the temp table
        	temp.put(array[0], array[1] + "/" + array[2]);
        	
        	DocForWord++;
     	}
     		
     	// Computing IDF
     	double IDF = Math.log(numberOfDocs / DocForWord);
     		
     	for (Entry<String, String> entry : temp.entrySet()) 
     	{
     		String docID = entry.getKey();
     		
     		String value = entry.getValue();
            String[] array = value.split("/");
               
            Integer.valueOf(array[1]);
                
     		// Computing TF-IDF
     		double TF = Integer.valueOf(array[0]) + Integer.valueOf(array[1]);
     		double TFIDF = TF * IDF;
     		String res = String.valueOf(TFIDF);
     			
     		String key = cleI.toString() + "," + docID;
     		
     		Text keyS = new Text(key);
     		Text result = new Text(res);
     		
     		context.write(keyS, result);
     
     	}
    }
}
