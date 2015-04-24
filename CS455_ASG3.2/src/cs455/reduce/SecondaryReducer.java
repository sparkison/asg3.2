/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cs455.util.RangeBuilder;

/*
 * Input format for Q7: "<rooms, '9 rooms=7701'>"
 * Input format for Q8: "<Aged 85 and greater, 'AL=13/407675'>"
 */
public class SecondaryReducer extends Reducer<Text, Text, Text, Text>  {

	private static Text result = new Text();
	private static Text word = new Text();
	private RangeBuilder rb = RangeBuilder.getInstance();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		int count2 = 0;
		int total = 0;
		float percentile = 0;
		int percentileCompare = 0;
		
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
				if (percent > greatestPercent && percent < 100){
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
			Map<String, Integer> valueMap = new HashMap<String, Integer>();
			String[] orderedRange = rb.getRoomValueRange();
			String percentileRange = "";
			String numRooms = "";

			// Get the counts for each range
			for (Text value : values) {
				String[] split = value.toString().split("=");
				numRooms = split[0].trim();
				count = Integer.parseInt(split[1]);
				percentile += count;
				if (!valueMap.containsKey(numRooms)) {
					valueMap.put(numRooms, count);
				} else {
					count2 = valueMap.get(numRooms);
					count2 += count;
					valueMap.put(numRooms, count2);
				}
			}
			
			total = (int) percentile;
			percentile = (float) (percentile * 0.95);

			// Loop through the ordered set to determine which range contains the 95th percentile
			for (int i = 0; i<orderedRange.length; i++) {
				percentileCompare += valueMap.get(orderedRange[i]);
				if (percentileCompare >= percentile) {
					percentileRange = orderedRange[i];
					break;
				}
			}

			result.set(percentileRange + " at " + (int) percentile + " of " + total);
			word.set("95th percentile of number of rooms");
			context.write(word, result);

		}

	}

	// Helper method to calculate percentage of two values
	private float getPercent(int num, int denom){
		float percent = num * 100f / denom;
		return percent;
	}

}
