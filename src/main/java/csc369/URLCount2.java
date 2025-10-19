package csc369;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLCount2 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] input = value.toString().split("\t");
            String out = input[0] + "\t" + input[1];
	        context.write(new Text(out), one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        @Override
	    protected void reduce(Text key, Iterable<IntWritable> intOne, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable one : intOne){
                sum++;
            }
            context.write(key, new IntWritable(sum));
       }
    }
    
}
