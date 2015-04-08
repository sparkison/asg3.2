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
 * <state@rent-own, "count-rented/count-owned"> 												– Used for Q1 analysis
 * <state@maleUnmarried-femaleUnmarried, "male-unmarried/female-unmarried/total-population"> 	– Used for Q2 analysis
 * <state@rural-urban, "count-rural/count-urban"> 												– Used for Q4 analysis
 * <state@male18-female18, "male-under18/female-under18/total-population"> 						– Used for Q3(a) analysis
 */
public class CensusMapper extends Mapper<LongWritable, Text, Text, Text> {

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
				
				/*
				 * Male-unarried vs. female-unmarried
				 */
				totalPop = Integer.parseInt(line.substring(300, 309));
				//				malePop = Integer.parseInt(line.substring(363, 372));
				//				femalePop = Integer.parseInt(line.substring(372, 381));

				maleUnmarried = Integer.parseInt(line.substring(4422, 4431)); 
				femaleUnmarried = Integer.parseInt(line.substring(4467, 4476));

				word.set(state + "@maleUnmarried-femaleUnmarried");
				output.set(maleUnmarried + "/" + femaleUnmarried + "/" + totalPop);
				context.write(word, output);
				
				/*
				 * Male 18 and under/femail 18 and under
				 */
				int maleUnder18 = 0;
				int femaleUnder18 = 0;
				int start, end;
				
				/*
				 * Males aged 18 and under
				 * Start index = 3864, end = 3981
				 * (3981-3864)/9 = 13
				 */
				start = 3864;
				for (int i = 0; i<13; i++) {
					end = start + 9;
					maleUnder18 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				
				/*
				 * Females aged 18 and under
				 * Start index = 4143, end = 4260
				 * (4260-4143)/9 = 13
				 */
				start = 4143;
				for (int i = 0; i<13; i++) {
					end = start + 9;
					femaleUnder18 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				
				word.set(state + "@male18-female18");
				output.set(maleUnder18 + "/" + femaleUnder18 + "/" + totalPop);
				context.write(word, output);

			}
			if (logicalRecordPart == 2) {
				// Rented vs. owned
				rented = Integer.parseInt(line.substring(1812, 1821));
				owned = Integer.parseInt(line.substring(1803, 1812));

				word.set(state + "@rent-own");
				output.set(rented + "/" + owned);
				context.write(word, output);

				// Urban vs. rural
				urbanInside = Integer.parseInt(line.substring(1857, 1866));
				urbanOutside = Integer.parseInt(line.substring(1866, 1875));
				rural = Integer.parseInt(line.substring(1875, 1884));
				
				word.set(state + "@rural-urban");
				output.set(rural + "/" + (urbanInside + urbanOutside));
				context.write(word, output);
			}

		}// END While loop

	}// END map

}
