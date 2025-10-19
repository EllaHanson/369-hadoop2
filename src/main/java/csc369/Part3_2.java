package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Part3_2 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, URLCountryPair, Text> {

        @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] input = value.toString().split("\t");
            String url = input[0];
            String country = input[1];
	        context.write(new URLCountryPair(url, country), new Text(country));
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(URLCountryPair.class, true); 
        }
        
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            URLCountryPair one = (URLCountryPair) wc1;
            URLCountryPair two = (URLCountryPair) wc2;
            return one.getURL().compareTo(two.getURL());
        }
    }
    

    public static class ReducerImpl extends Reducer<URLCountryPair, Text, Text, Text> {
    
        @Override
	    protected void reduce(URLCountryPair key, Iterable<Text> countries, Context context) throws IOException, InterruptedException {
            
            String url = key.getURL().toString();
            String val = null;
            String prev = null;

            for (Text country : countries){
                String curr = country.toString();
                if (prev == null || !curr.equals(prev)) {
                    if (val == null) {
                        val = curr;
                    }
                    else {
                        val = val + ", " + country;
                    }
                    prev = curr;
                }
            }
            context.write(new Text(url), new Text(val));
       }
    }
    
}
