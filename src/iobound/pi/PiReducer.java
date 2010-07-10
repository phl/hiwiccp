package iobound.pi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PiReducer extends Reducer<IntWritable, IntWritable, KEYOUT, VALUEOUT>{

}
