/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CensusVersusReducer extends Reducer<Text, Text, Text, Text> {

	private static Text result = new Text();
	private static Text word = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		int count2 = 0;
		int total = 0;
		
		// Used to determine what type of analysis we're doing
		String[] type = key.toString().split("@");
		
		/*
		 * If here, doing rent vs. owned comparison
		 */
		if (type[1].trim().equals("rent-own")) {
			
			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
			}
			
			total = count + count2;
			
			word.set(key.toString().split("@")[0] + " % Rent");
			result.set(count + "/" + total);
			context.write(word, result);
			
			word.set(key.toString().split("@")[0] + " % Own");
			result.set(count2 + "/" + total);
			context.write(word, result);
			
		} 
		/*
		 * If here, doing male-unmarried vs. female-unmarried comparison
		 */
		else if (type[1].trim().equals("maleUnmarried-femaleUnmarried")) {
			
			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}
						
			word.set(key.toString().split("@")[0] + " % Male never married");
			result.set(count + "/" + total);
			context.write(word, result);
			
			word.set(key.toString().split("@")[0] + " % Female never married");
			result.set(count2 + "/" + total);
			context.write(word, result);
			
		}

	}
	
}
