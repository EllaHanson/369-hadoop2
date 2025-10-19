package csc369;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLCountSort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCountPair, Text> {

        @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] input = value.toString().split("\t");
            String country = input[0];
            String url = input[1];
            int count = Integer.parseInt(input[2]);
	        context.write(new CountryCountPair(country, count), new Text(url));
        }
    }
    

    public static class ReducerImpl extends Reducer<CountryCountPair, Text, Text, IntWritable> {
    
        @Override
	    protected void reduce(CountryCountPair key, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
            for (Text curr_url: urls){
                String country = key.getCountry().toString();
                String url = curr_url.toString();
                String out = country + "\t" + url;

                context.write(new Text(out), key.getCount());
            }
       }
    }
    
}
