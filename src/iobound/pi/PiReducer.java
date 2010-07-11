package iobound.pi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PiReducer extends Reducer<LongWritable, LongWritable, Text, DoubleWritable> {

	
	public void reduce(LongWritable numTested, Iterable<LongWritable> coprimeTotals, Context context) 
		throws IOException, InterruptedException {
		
		long totalTested = 0;
		long totalCoprime = 0;
		
		for (LongWritable numComprime : coprimeTotals) {
			
			totalTested += numTested.get();
			totalCoprime += numComprime.get();
		}
		
		context.write(new Text("PI"), new DoubleWritable(estimatePi(totalTested, totalCoprime)));
	}

	private double estimatePi(long totalTested, long totalCoprime) {
		System.out.println(totalTested+", "+totalCoprime);
		return Math.sqrt(6 * (double)totalTested / totalCoprime);
	}
}
