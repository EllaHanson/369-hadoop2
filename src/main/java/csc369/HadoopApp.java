package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCount".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, CountryCount.LogMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, CountryCount.CountryMapper.class ); 

	    job.setReducerClass(CountryCount.JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("CountrySum".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CountrySum.ReducerImpl.class);
	    job.setMapperClass(CountrySum.MapperImpl.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(CountrySum.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountrySum.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountrySort".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CountrySort.ReducerImpl.class);
	    job.setMapperClass(CountrySort.MapperImpl.class);
		job.setMapOutputKeyClass(IntWritable.class);
    	job.setMapOutputValueClass(Text.class); 
		job.setSortComparatorClass(CountrySort.SortComparator.class);
	    job.setOutputKeyClass(CountrySort.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountrySort.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("URLCount".equalsIgnoreCase(otherArgs[0])) {
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, URLCount.LogMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, URLCount.CountryMapper.class ); 

	    job.setReducerClass(URLCount.JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(URLCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(URLCount.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("URLCount2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(URLCount2.ReducerImpl.class);
	    job.setMapperClass(URLCount2.MapperImpl.class);
	    job.setOutputKeyClass(URLCount2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(URLCount2.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("URLCountSort".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(URLCountSort.ReducerImpl.class);
	    job.setMapperClass(URLCountSort.MapperImpl.class);
	    job.setOutputKeyClass(URLCountSort.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(URLCountSort.OUTPUT_VALUE_CLASS);
		job.setMapOutputKeyClass(CountryCountPair.class);
    	job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Part3_1".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, Part3_1.LogMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, Part3_1.CountryMapper.class ); 

	    job.setReducerClass(Part3_1.JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(Part3_1.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Part3_1.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("Part3_2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Part3_2.ReducerImpl.class);
	    job.setMapperClass(Part3_2.MapperImpl.class);

	    job.setOutputKeyClass(Part3_2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Part3_2.OUTPUT_VALUE_CLASS);

		job.setGroupingComparatorClass(Part3_2.GroupingComparator.class);

		job.setMapOutputKeyClass(URLCountryPair.class);
    	job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
