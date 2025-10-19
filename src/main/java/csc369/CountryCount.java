package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryCount {
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

	// Mapper for log file
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    	String[] input = value.toString().split(" ");
			String host = input[0]; 
	    	String out = "A\t1";
	    	context.write(new Text(host), new Text(out));
		} 
    }

    // Mapper for country file
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] input = value.toString().split(",");
			String host = input[0];
			String country = input[1];
			String out = "B\t" + country;
			context.write(new Text(host), new Text(out));
		}
    }

    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {
	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			String country = null;
			int count = 0;
	    	for (Text val : values) {
				String[] input = val.toString().split("\t");
				String c = input[0];
				if (c.equals("B")) {
					country = input[1];
				}
				if (c.equals("A")) {
					count++;
				}
	    	}
			if ((country != null) && (count > 0)) {
				context.write(new Text(country), new IntWritable(count));
			}
		}
    } 
    
}
