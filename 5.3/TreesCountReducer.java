package cs.bigdata.Tutorial2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
//import java.io.Iterable;
import java.util.Iterator;


public class TreesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Overriding of the reduce function
	private IntWritable count = new IntWritable();

    protected void reduce(Text cleI, Iterable<IntWritable> listevalI, Context context) throws IOException,InterruptedException
    {

        int total = 0;
        
        for (IntWritable val: listevalI) {
        	total += val.get();
        }
        
        count.set(total);
        context.write(cleI, count);

    }
}
