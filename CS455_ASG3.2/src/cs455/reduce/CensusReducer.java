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

/*
 * Input formats: 
 * <state@rent-own, "count-rented/count-owned"> 												– Used for Q1 analysis
 * <state@maleUnmarried-femaleUnmarried, "male-unmarried/female-unmarried/total-population"> 	– Used for Q2 analysis
 * <state@rural-urban, "count-rural/count-urban"> 												– Used for Q4 analysis
 * <state@male18-female18, "male-under18/female-under18/total-population"> 						– Used for Q3(a) analysis
 * <state@male19to29-female19to29, "male-19to29/female-19to29/total-population"> 				– Used for Q3(b) analysis
 * <state@male30to39-female30to39, "male-30to39/female-30to39/total-population"> 				– Used for Q3(c) analysis
 * <state@home-value, "value-range/count-of-range"> 											– Used for Q5 analysis
 * <state@rent-value, "value-range/count-of-range"> 											– Used for Q6 analysis
 */
public class CensusReducer extends Reducer<Text, Text, Text, Text> {

	private static Text result = new Text();
	private static Text word = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		int count2 = 0;
		int total = 0;

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
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}

			word.set(type[0] + " % Male never married (of total pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female never married (of total pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(4) Urban vs. rural
		 *************************************/
		if (inputType.equals("rural-urban")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
			}

			total = count + count2;

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
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}

			word.set(type[0] + " % Male 18 and under (of total pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female 18 and under (of total pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(3b) Male 19 to 29/female 19 to 29
		 *************************************/
		if (inputType.equals("male19to29-female19to29")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}

			word.set(type[0] + " % Male age 19 to 29 (of total pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female age 19 to 29 (of total pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(3c) Male 30 to 39/female 30 to 39
		 *************************************/
		if (inputType.equals("male30to39-female30to39")) {

			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}

			word.set(type[0] + " % Male age 30 to 39 (of total pop)");
			result.set(count + "/" + total + " = " + getPercent(count, total) + "%");
			context.write(word, result);

			word.set(type[0] + " % Female age 30 to 39 (of total pop)");
			result.set(count2 + "/" + total + " = " + getPercent(count2, total) + "%");
			context.write(word, result);

		}

		/*************************************
		 * Q(5) Median house value (owner occupied)
		 *************************************/
		if (inputType.equals("home-value")) {

			Map<String, Integer> houseValMap = new HashMap<String, Integer>();
			Map<Integer, String> sortedValMap = new TreeMap<Integer, String>();
			String valRange;

			for (Text value : values) {
				String[] split = value.toString().split("=");
				valRange = split[0].trim();
				if (!houseValMap.containsKey(valRange)) {
					houseValMap.put(valRange, Integer.parseInt(split[1]));
				} else {
					count = houseValMap.get(valRange);
					count += Integer.parseInt(split[1]);
					houseValMap.put(valRange, count);
				}
			}

			// Sort the results by value
			for (String range : houseValMap.keySet()) {
				sortedValMap.put(houseValMap.get(range), range);
			}
			// Add sorted keys to list to get specific indices
			List<Integer> sortedIndex = new ArrayList<Integer>();
			for (Integer index : sortedValMap.keySet()) {
				sortedIndex.add(index);
			}

			/*
			 * List size is 20, so grab the two middle values, 
			 * median range will be between highest and lowest of these
			 * two ranges (?)
			 * 
			 * Or, just grab index 9, which has the higher count of the two
			 * middle values and call it the median...
			 */
			
			//			DecimalFormat formatter = new DecimalFormat("#,###");
			//			String rangeOne = sortedValMap.get(sortedIndex.get(9));
			//			String rangeTwo = sortedValMap.get(sortedIndex.get(10));
			//
			//			List<Integer> rangeList = new ArrayList<Integer>();
			//			rangeList.add(Integer.parseInt(rangeOne.split(" - ")[0].replaceAll("[^0-9]", "")));
			//			rangeList.add(Integer.parseInt(rangeOne.split(" - ")[1].replaceAll("[^0-9]", "")));
			//			rangeList.add(Integer.parseInt(rangeTwo.split(" - ")[0].replaceAll("[^0-9]", "")));
			//			rangeList.add(Integer.parseInt(rangeTwo.split(" - ")[1].replaceAll("[^0-9]", "")));
			//			Collections.sort(rangeList);
			//			
			//			word.set(type[0] + " median house value");
			//			if (rangeList.get(3) > 500000)
			//				result.set("$" + formatter.format(rangeList.get(0)) + " - more than $500,000");
			//			else
			//				result.set("$" + formatter.format(rangeList.get(0)) + " - $" + formatter.format(rangeList.get(3)));
			//			context.write(word, result);

			result.set(sortedValMap.get(sortedIndex.get(9)));
			word.set(type[0] + " median house value");
			context.write(word, result);

		}

		/*************************************
		 * Q(6) Median rent paid
		 *************************************/
		if (inputType.equals("rent-value")) {

			Map<String, Integer> houseValMap = new HashMap<String, Integer>();
			Map<Integer, String> sortedValMap = new TreeMap<Integer, String>();
			String valRange;

			for (Text value : values) {
				String[] split = value.toString().split("=");
				valRange = split[0].trim();
				if (!houseValMap.containsKey(valRange)) {
					houseValMap.put(valRange, Integer.parseInt(split[1]));
				} else {
					count = houseValMap.get(valRange);
					count += Integer.parseInt(split[1]);
					houseValMap.put(valRange, count);
				}
			}

			// Sort the results by value
			for (String range : houseValMap.keySet()) {
				sortedValMap.put(houseValMap.get(range), range);
			}
			List<Integer> sortedIndex = new ArrayList<Integer>();
			for (Integer index : sortedValMap.keySet()) {
				sortedIndex.add(index);
			}

			/*
			 * List size is 17, so simply grab the middle value
			 * from the sorted array
			 */
			word.set(type[0] + " median rent paid");
			result.set(sortedValMap.get(sortedIndex.get(8)));
			context.write(word, result);

		}

	}

	// Helper method to calculate percentage of two values
	private float getPercent(int num, int denom){
		float percent = num * 100f / denom;
		return percent;
	}

}
