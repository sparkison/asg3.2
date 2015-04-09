/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CensusPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		// Used to determine what type of analysis we're doing
		String[] type = key.toString().split("@");
		String inputType = type[1].trim();

		// Q1
		if (inputType.equals("rent-own")) {
			return 0;
		}
		// Q2
		if (inputType.equals("maleUnmarried-femaleUnmarried")) {
			return 1;
		}
		// Q3(a)
		if (inputType.equals("male18-female18")) {
			return 2;
		}
		// Q3(b)
		if (inputType.equals("male19to29-female19to29")) {
			return 2;
		}
		// Q3(c)
		if (inputType.equals("male30to39-female30to39")) {
			return 2;
		}
		// Q4
		if (inputType.equals("rural-urban")) {
			return 3;
		}
		// Q5
		if (inputType.equals("home-value")) {
			return 4;
		}
		// Q6
		if (inputType.equals("rent-value")) {
			return 5;
		}
		// Q7
		if (inputType.equals("number-rooms")) {
			return 6;
		}
		// Q8
		if (inputType.equals("maleOver85-femalOver85")) {
			return 7;
		}

		return 0;

	}

}
