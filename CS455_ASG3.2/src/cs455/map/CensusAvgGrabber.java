/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Output formats: 
 * 
 */
public class CensusAvgGrabber extends Mapper<LongWritable, Text, Text, Text> {

	private final static int MAX_LEVEL = 100;
	private static Text word = new Text();
	private static Text output = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String state;
		int summaryLevel, logicalRecord, logicalRecordPart, totalRecordParts;
		int totalPop, malePop, femalePop;
		int owned, rented;
		int maleUnmarried, femaleUnmarried;
		int urbanInside, urbanOutside, rural;

		// Split on newline
		StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
		// Iterate through each line and process
		while(lineItr.hasMoreTokens()){

			/*
			 * ***************************************************
			 * Primary file information
			 * ***************************************************
			 */

			String line = lineItr.nextToken();
			state = line.substring(8, 10);
			summaryLevel = Integer.parseInt(line.substring(10, 13));

			// Only analyze up to a summary level of 100 
			if (summaryLevel != MAX_LEVEL)
				continue;

			// Get record information
			logicalRecord = Integer.parseInt(line.substring(18, 24));
			logicalRecordPart = Integer.parseInt(line.substring(24, 28));
			totalRecordParts = Integer.parseInt(line.substring(28, 32));

			// Sanity check; used for debugging
			String result = "State:\t" + state + 
					", Summary level:\t" + summaryLevel + 
					", Logical Record:\t" + logicalRecord + 
					", Record Part:\t" + logicalRecordPart + 
					", Total Record Parts:\t" + totalRecordParts;


			/*
			 * ***************************************************
			 * END Primary file information
			 * ***************************************************
			 */			

			if (logicalRecordPart == 1) {
				//TODO do something
			}
			if (logicalRecordPart == 2) {
				//TODO do something
			}

		}// END While loop

	}// END map
	
}
