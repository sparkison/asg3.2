/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.util.RangeBuilder;

/*
 * Input formats: 
 * <state@rent-own, "count-rented/count-owned"> 													– Used for Q1 analysis
 * <state@maleUnmarried-femaleUnmarried, "male-unmarried/female-unmarried/total-male/total-female"> – Used for Q2 analysis
 * <state@rural-urban, "count-rural/count-urban/count-undefined">									– Used for Q4 analysis
 * <state@male18-female18, "male-under18/female-under18/total-male/total-female"> 					– Used for Q3(a) analysis
 * <state@male19to29-female19to29, "male-19to29/female-19to29/total-male/total-female"> 			– Used for Q3(b) analysis
 * <state@male30to39-female30to39, "male-30to39/female-30to39/total-male/total-female"> 			– Used for Q3(c) analysis
 * <state@home-value, "value-range=count-of-range"> 												– Used for Q5 analysis
 * <state@rent-value, "value-range=count-of-range"> 												– Used for Q6 analysis
 * <state@number-rooms, "number-of-rooms=count"> 													– Used for Q7 analysis
 * <state@maleOver85-femalOver85, "male-85-and-older/female-85-and-older/total-population">			– Used for Q8 analysis
 */
public class CensusReducer extends Reducer<Text, Text, Text, Text> {

	private static Text result = new Text();
	private static Text word = new Text();
	private RangeBuilder rb = RangeBuilder.getInstance();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		int count2 = 0;
		int total = 0;
		int total2 = 0;
		float percentile = 0;
		int percentileCompare = 0;

		// Used to determine what type of analysis we're doing
		String[] type = key.toString().split("@");
		String inputType = type[1].trim();

		/*************************************
		 * Q(1) Rented vs. owned
		 *************************************/
		if (inputType.equals("rent-own")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
			}

			total = count + count2;

			word.set(type[0] + " % Rent");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Own");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(2) Male-unarried vs. female-unmarried
		 *************************************/
		if (inputType.equals("maleUnmarried-femaleUnmarried")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				// Count of male unmarried
				count += Integer.parseInt(split[0]);
				// Count of female unmarried
				count2 += Integer.parseInt(split[1]);
				// Count of population male
				total += Integer.parseInt(split[2]);
				// Count of population female
				total2 += Integer.parseInt(split[3]);
			}

			word.set(type[0] + " % Male never married (of total pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female never married (of total pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total2) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(4) Urban vs. rural
		 *************************************/
		if (inputType.equals("rural-urban")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				// Count of rural
				count += Integer.parseInt(split[0]);
				// Count of urban
				count2 += Integer.parseInt(split[1]);
				// Count of not-defined
				total += Integer.parseInt(split[2]);
			}
			
			// Add rural and urban to not-defined to get total count
			total += (count + count2);

			word.set(type[0] + " % Rural households");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Urban households");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(3a) Male 18 and under/female 18 and under
		 *************************************/
		if (inputType.equals("male18-female18")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				// Count of male 18 and under
				count += Integer.parseInt(split[0]);
				// Count of female 18 and under
				count2 += Integer.parseInt(split[1]);
				// Count of population male
				total += Integer.parseInt(split[2]);
				// Count of population female
				total2 += Integer.parseInt(split[3]);
			}

			word.set(type[0] + " % Male 18 and under (of male pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female 18 and under (of female pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total2) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(3b) Male 19 to 29/female 19 to 29
		 *************************************/
		if (inputType.equals("male19to29-female19to29")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				// Count of male 19 to 29
				count += Integer.parseInt(split[0]);
				// Count of female 19 to 29
				count2 += Integer.parseInt(split[1]);
				// Count of population male
				total += Integer.parseInt(split[2]);
				// Count of population female
				total2 += Integer.parseInt(split[3]);
			}

			word.set(type[0] + " % Male age 19 to 29 (of male pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female age 19 to 29 (of female pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total2) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(3c) Male 30 to 39/female 30 to 39
		 *************************************/
		if (inputType.equals("male30to39-female30to39")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				// Count of male 30 to 39
				count += Integer.parseInt(split[0]);
				// Count of female 30 to 39
				count2 += Integer.parseInt(split[1]);
				// Count of population male
				total += Integer.parseInt(split[2]);
				// Count of population female
				total2 += Integer.parseInt(split[3]);
			}

			word.set(type[0] + " % Male age 30 to 39 (of male pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female age 30 to 39 (of female pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total2) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(5) Median house value (owner occupied)
		 *************************************/
		if (inputType.equals("home-value")) {

			Map<String, Integer> valueMap = new HashMap<String, Integer>();
			String[] orderedRange = rb.getHouseValueRanges();
			String percentileRange = "";

			// Get the counts for each value range
			for (Text value : values) {
				String[] split = value.toString().split("=");
				String valRange = split[0].trim();
				count = Integer.parseInt(split[1]);
				percentile += count;
				if (!valueMap.containsKey(valRange)) {
					valueMap.put(valRange, count);
				} else {
					count2 = valueMap.get(valRange);
					count2 += count;
					valueMap.put(valRange, count2);
				}
			}
			percentile = (float) (percentile * 0.5);

			// Loop through the ordered set to determine which range contains the percentile
			for (int i = 0; i<orderedRange.length; i++) {
				percentileCompare += valueMap.get(orderedRange[i]);
				if (percentileCompare >= percentile) {
					percentileRange = orderedRange[i];
					break;
				}
			}

			result.set(percentileRange);
			word.set(type[0] + " median house value");
			context.write(word, result);

		}

		/*************************************
		 * Q(6) Median rent paid
		 *************************************/
		if (inputType.equals("rent-value")) {

			Map<String, Integer> valueMap = new HashMap<String, Integer>();
			String[] orderedRange = rb.getHouseRentRanges();
			String percentileRange = "";

			// Get the counts for each value range
			for (Text value : values) {
				String[] split = value.toString().split("=");
				String valRange = split[0].trim();
				count = Integer.parseInt(split[1]);
				percentile += count;
				if (!valueMap.containsKey(valRange)) {
					valueMap.put(valRange, count);
				} else {
					count2 = valueMap.get(valRange);
					count2 += count;
					valueMap.put(valRange, count2);
				}
			}
			percentile = (float) (percentile * 0.5);

			// Loop through the ordered set to determine which range contains the percentile
			for (int i = 0; i<orderedRange.length; i++) {
				percentileCompare += valueMap.get(orderedRange[i]);
				if (percentileCompare >= percentile) {
					percentileRange = orderedRange[i];
					break;
				}
			}

			result.set(percentileRange);
			word.set(type[0] + " median rent paid");
			context.write(word, result);

		}

		/*************************************
		 * Q(7) 95'th percentile number of rooms
		 *************************************/
		if (inputType.equals("number-rooms")) {

			Map<String, Integer> valueMap = new HashMap<String, Integer>();
			
			for (Text value : values) {
				String[] split = value.toString().split("=");
				String numRooms = split[0].trim();
				count = Integer.parseInt(split[1]);
				if (!valueMap.containsKey(numRooms)) {
					valueMap.put(numRooms, count);
				} else {
					count2 = valueMap.get(numRooms);
					count2 += count;
					valueMap.put(numRooms, count2);
				}
			}
			
			for (String val : valueMap.keySet()) {
				result.set("" + valueMap.get(val));
				word.set(type[0] + " " + val);
				context.write(word, result);
			}

		}

		/*************************************
		 * Q(8) Population over 85
		 *************************************/
		if (inputType.equals("maleOver85-femalOver85")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				total += Integer.parseInt(split[1]);
			}

			word.set(type[0] + " Aged 85 and greater");
			result.set(count + "/" + total);
			context.write(word, result);

		}

	}

	// Helper method to calculate percentage of two values
	private float getPercent(int num, int denom){
		float percent = num * 100f / denom;
		return percent;
	}

}
