package cs.bigdata.Tutorial2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
//import java.io.Iterable;
import java.util.Iterator;


public class PageRankReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    // Overriding of the reduce function
	private DoubleWritable highest = new DoubleWritable();

    protected void reduce(Text cleI, Iterable<IntWritable> listevalI, Context context) throws IOException,InterruptedException
    {

		// 
		
        context.write(cleI, highest);

    }
}
