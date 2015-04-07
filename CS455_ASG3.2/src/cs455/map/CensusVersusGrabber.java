/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 * This Map is used to grab all relevant fields.
 * Only used as a starting point to widdle down for
 * other map tasks.
 */

package cs455.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Output formats: 
 * <state@rent-own, "count-rented/count-owned">
 * <state@maleUnmarried-femaleUnmarried, "male-unmarried/female-unmarried/total-population">
 */
public class CensusVersusGrabber extends Mapper<LongWritable, Text, Text, Text> {

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
						
			// Sanity check
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
				// Population count
				totalPop = Integer.parseInt(line.substring(300, 309));
				malePop = Integer.parseInt(line.substring(363, 372));
				femalePop = Integer.parseInt(line.substring(372, 381));
				
				maleUnmarried = Integer.parseInt(line.substring(4422, 4431)); 
				femaleUnmarried = Integer.parseInt(line.substring(4467, 4476));
				
				word.set(state + "@maleUnmarried-femaleUnmarried");
				output.set(maleUnmarried + "/" + femaleUnmarried + "/" + totalPop);
				context.write(word, output);
				
			}
			if (logicalRecordPart == 2) {
				rented = Integer.parseInt(line.substring(1812, 1821));
				owned = Integer.parseInt(line.substring(1803, 1812));
				
				word.set(state + "@rent-own");
				output.set(rented + "/" + owned);
				context.write(word, output);

			}
					
		}// END While loop
		
	}// END map
	
}
