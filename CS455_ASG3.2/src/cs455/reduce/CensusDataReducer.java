package cs455.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CensusDataReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

	private static IntWritable result = new IntWritable();
	
	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//TODO

	}
	
}
