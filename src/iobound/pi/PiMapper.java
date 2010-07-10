package iobound.pi;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class PiMapper extends Mapper<IntWritable,Void,IntWritable,IntWritable> {
	
	static enum MyCounters { NUM_RECORDS }

	private String mapTaskId;
	private String inputFile;
	private Random random;
	private int numRecords = 0;

	public void configure(JobConf job) {
		mapTaskId = job.get("mapred.task.id");
		inputFile = job.get("map.input.file");
		random = new Random(System.currentTimeMillis());
	}


	public void map(Object k, Object v,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
	throws IOException {
		
		IntWritable key = new IntWritable(Math.abs(random.nextInt()));
		IntWritable val = new IntWritable(Math.abs(random.nextInt()));
		
		// Increment the no. of <key, value> pairs processed
		++numRecords;

		// Increment counters
		reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

		// Every 100 records update application-level status
		if ((numRecords%100) == 0) {
			reporter.setStatus(mapTaskId + " generated " + numRecords + " integers.");
			reporter.progress();
		}

		// Output the result
		output.collect(key, val);
	}
}
