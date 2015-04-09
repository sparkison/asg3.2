/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SecondaryMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Text word = new Text();
	private static Text output = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// The K,V pair from previous job
		String[] split = value.toString().split("\t");
				
		if (split[0].substring(3, 7).equals("Aged")) {
			/*
			 * Format of input: "AL Aged 85 and greater	13/407675"
			 * Format of output: "<Aged 85 and greater, AL=13/407675>"
			 */
			word.set("Aged 85 and greater");
			output.set(split[0].substring(0, 2) + "=" + split[1]);
			context.write(word, output);
		} else {
			/*
			 * Format of input: "AL 9 rooms	7701"
			 * Format of output: "<9 rooms, AL=7701>"
			 */
			word.set(split[0].substring(3, split[0].length()));
			output.set(split[0].substring(0, 2) + "=" + split[1]);
			context.write(word, output);
		}
		
	}
	
}
