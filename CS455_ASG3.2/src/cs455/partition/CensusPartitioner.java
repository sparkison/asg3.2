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
		String versusType = type[1].trim();

		// Q1
		if (versusType.equals("rent-own")) {
			return 0;
		}
		// Q2
		if (versusType.equals("maleUnmarried-femaleUnmarried")) {
			return 1;
		}
		// Q3(a)
		if (versusType.equals("male18-female18")) {
			return 2;
		}
		// Q3(b)
		if (versusType.equals("male19to29-female19to29")) {
			return 2;
		}
		// Q3(c)
		if (versusType.equals("male30to39-female30to39")) {
			return 2;
		}
		// Q4
		if (versusType.equals("rural-urban")) {
			return 3;
		}
		// Q5
		if (versusType.equals("home-value")) {
			return 4;
		}

		return 0;

	}

}
