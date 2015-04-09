/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * Input format for Q7: "<9 rooms, AL=7701>"
 * Input format for Q8: "<Aged 85 and greater, AL=13/407675>"
 */
public class SecondaryReducer extends Reducer<Text, Text, Text, Text>  {

	private static Text result = new Text();
	private static Text word = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/*************************************
		 * Q(8) Population over 85
		 *************************************/
		if (key.toString().startsWith("Aged")) {

			Float greatestPercent = Float.MIN_VALUE;
			String state = "";

			for (Text value : values) {
				String[] split = value.toString().split("=");
				int num = Integer.parseInt(split[1].split("/")[0]);
				int denom = Integer.parseInt(split[1].split("/")[1]);
				float percent = getPercent(num, denom);
				if (percent > greatestPercent){
					greatestPercent = percent;
					state = split[0];
				}

			}

			word.set("State with greatest percent aged 85 and older is " + state);
			result.set("" + greatestPercent + "%");
			context.write(word, result);

		} 
		/*************************************
		 * Q(7) 95'th percentile number of rooms
		 *************************************/
		else {
			
			for (Text value : values) {
				//TODO
			}

			word.set("");
			result.set("");
			context.write(word, result);

		}

	}

	// Helper method to calculate percentage of two values
	private float getPercent(int num, int denom){
		float percent = num * 100f / denom;
		return percent;
	}

}
