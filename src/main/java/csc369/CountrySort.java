package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountrySort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] input = value.toString().split("\t");
            String country = input[0];
            int sum = Integer.parseInt(input[1]);
	        context.write(new IntWritable(sum), new Text(country));
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true); // sorting an intwritable
        }
        
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            IntWritable one = (IntWritable) wc1;
            IntWritable two = (IntWritable) wc2;
            return -one.compareTo(two);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, Text, IntWritable> {
    
        @Override
	    protected void reduce(IntWritable sums, Iterable<Text> countries, Context context) throws IOException, InterruptedException {
            for (Text country : countries) {
                context.write(country, sums);
            }
        }
    }
    
}
