package iobound.pi;

import static java.lang.Math.abs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoprimeMapper extends Mapper<LongWritable,Text,LongWritable,LongWritable> {
	
	static enum MyCounters { NUM_INVOCATIONS }

	private String mapTaskId;
	private Random random;
	private int numInvocations = 0;

	@Override
	public void setup(Context ctx) {
		mapTaskId = ctx.getTaskAttemptID().toString();
		random = new Random(System.currentTimeMillis());
	}

	@Override
	public void map(LongWritable k, Text v, Context ctx)
		throws IOException, InterruptedException {
		
		int numTests=0;
		int numCoprime=0;
		
		for (int i=0; i < 1000000; i++) {
			if (areCoprime(abs(random.nextLong()), abs(random.nextLong()))) {
				++numCoprime;
			}
			
			++numTests;
			
			// Every 1000 records update application-level status
			if ((numTests%10000) == 0) {
				ctx.setStatus(mapTaskId + " tested " + (numInvocations*1000000 + numTests) + " integer pairs.");
				ctx.progress();
			}
		}
		
		// Increment the no. of <key, value> pairs processed
		++numInvocations;

		// Increment counters
	
		// Output the result
		ctx.write(new LongWritable(numTests), new LongWritable(numCoprime));
	}
	
	boolean areCoprime(long a, long b) {
		return 1 == gcd(a, b);
	}
	
	long gcd(long a, long b) {
		return b==0 ? a : gcd(b, a%b);
	}
}
