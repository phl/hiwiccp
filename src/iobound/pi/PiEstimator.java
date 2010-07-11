package iobound.pi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PiEstimator {
	
	public static void main(String... args) throws Exception {
		
		Job job = new Job();
		//job.setJarByClass(PiEstimator.class);

		FileInputFormat.addInputPath(job, new Path("/tmp/pi-input.txt"));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/tmp/pi-estimate"));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(CoprimeMapper.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(PiReducer.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
