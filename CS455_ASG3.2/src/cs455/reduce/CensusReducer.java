/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Input formats: 
 * <state@rent-own, "count-rented/count-owned"> 												– Used for Q1 analysis
 * <state@maleUnmarried-femaleUnmarried, "male-unmarried/female-unmarried/total-population"> 	– Used for Q2 analysis
 * <state@rural-urban, "count-rural/count-urban"> 												– Used for Q4 analysis
 * <state@male18-female18, "male-under18/female-under18/total-population"> 						– Used for Q3(a) analysis
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
		String versusType = type[1].trim();
		
		/*
		 * If here, doing rent vs. owned comparison
		 */
		if (versusType.equals("rent-own")) {
			
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
		if (versusType.equals("maleUnmarried-femaleUnmarried")) {
			
			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}
						
			word.set(key.toString().split("@")[0] + " % Male never married (of total pop)");
			result.set(count + "/" + total);
			context.write(word, result);
			
			word.set(key.toString().split("@")[0] + " % Female never married (of total pop)");
			result.set(count2 + "/" + total);
			context.write(word, result);
			
		}
		/*
		 * If here, doing rural vs. urban comparison
		 */
		if (versusType.equals("rural-urban")) {
			
			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
			}
			
			total = count + count2;
			
			word.set(key.toString().split("@")[0] + " % Rural households");
			result.set(count + "/" + total);
			context.write(word, result);
			
			word.set(key.toString().split("@")[0] + " % Urban households");
			result.set(count2 + "/" + total);
			context.write(word, result);
			
		}
		/*
		 * If here, doing male/female percent under 18
		 */
		if (versusType.equals("male18-female18")) {
			
			for (Text value : values) {
				String[] split = value.toString().split("/");
				count += Integer.parseInt(split[0]);
				count2 += Integer.parseInt(split[1]);
				total += Integer.parseInt(split[2]);
			}
						
			word.set(key.toString().split("@")[0] + " % Male 18 and under (of total pop)");
			result.set(count + "/" + total);
			context.write(word, result);
			
			word.set(key.toString().split("@")[0] + " % Female 18 and under (of total pop)");
			result.set(count2 + "/" + total);
			context.write(word, result);
			
		}

	}
	
}
