package cs455.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CensusDataReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private static IntWritable result = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		
		for (IntWritable value : values) {
			count += value.get();
		}
		
		result.set(count);
		context.write(key, NullWritable.get());

	}
	
}
