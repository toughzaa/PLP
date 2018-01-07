package cs.bigdata.Tutorial2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
//import java.io.Iterable;
import java.util.Iterator;


public class TreeHighestReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    // Overriding of the reduce function
	private DoubleWritable highest = new DoubleWritable();

    protected void reduce(Text cleI, Iterable<IntWritable> listevalI, Context context) throws IOException,InterruptedException
    {

        double max_height = 0;
        
        for (IntWritable val: listevalI) {
        	if(val.get() > max_height) {
        		max_height = val.get();
        	}
        }
        
        DoubleWritable highest = new DoubleWritable(max_height);
        
        context.write(cleI, highest);

    }
}
