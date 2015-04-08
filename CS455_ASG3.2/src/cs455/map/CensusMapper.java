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
 * <state@male19to29-female19to29, "male-19to29/female-19to29/total-population"> 				– Used for Q3(b) analysis
 * <state@male30to39-female30to39, "male-30to39/female-30to39/total-population"> 				– Used for Q3(c) analysis
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

			// Only analyze at summary level 100 
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

				/*************************************
				 * Q(2) Male-unarried vs. female-unmarried
				 *************************************/
				
				totalPop = Integer.parseInt(line.substring(300, 309));
				malePop = Integer.parseInt(line.substring(363, 372));
				femalePop = Integer.parseInt(line.substring(372, 381));

				maleUnmarried = Integer.parseInt(line.substring(4422, 4431)); 
				femaleUnmarried = Integer.parseInt(line.substring(4467, 4476));

				word.set(state + "@maleUnmarried-femaleUnmarried");
				output.set(maleUnmarried + "/" + femaleUnmarried + "/" + totalPop);
				context.write(word, output);

				/*************************************
				 * Q(3a) Male 18 and under/female 18 and under
				 *************************************/
				
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

				/*************************************
				 * Q(3b) Male 19 to 29/female 19 to 29
				 *************************************/
				
				int male19to29 = 0;
				int female19to29 = 0;
				/*
				 * Males aged 19 to 29
				 * Start index = 3981, end = 4026
				 * (4026-3981)/9 = 5
				 */
				start = 3981;
				for (int i = 0; i<5; i++) {
					end = start + 9;
					male19to29 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				/*
				 * Females aged 19 to 29
				 * Start index = 4260, end = 4305
				 * (4305-4260)/9 = 5
				 */
				start = 4260;
				for (int i = 0; i<5; i++) {
					end = start + 9;
					female19to29 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				word.set(state + "@male19to29-female19to29");
				output.set(male19to29 + "/" + female19to29 + "/" + totalPop);
				context.write(word, output);

				
				/*************************************
				 * Q(3c) Male 30 to 39/female 30 to 39
				 *************************************/
				
				int male30to39 = 0;
				int female30to39 = 0;
				/*
				 * Males aged 30 to 39
				 * Start index = 4026, end = 4044
				 * (4044-4026)/9 = 2
				 */
				start = 4026;
				for (int i = 0; i<2; i++) {
					end = start + 9;
					male30to39 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				/*
				 * Females aged 30 to 39
				 * Start index = 4305, end = 4323
				 * (4323-4305)/9 = 2
				 */
				start = 4305;
				for (int i = 0; i<2; i++) {
					end = start + 9;
					female30to39 += Integer.parseInt(line.substring(start, end));
					start += 9;
				}
				word.set(state + "@male30to39-female30to39");
				output.set(male30to39 + "/" + female30to39 + "/" + totalPop);
				context.write(word, output);

			}
			if (logicalRecordPart == 2) {
				/*************************************
				 * Q(1) Rented vs. owned
				 *************************************/
				rented = Integer.parseInt(line.substring(1812, 1821));
				owned = Integer.parseInt(line.substring(1803, 1812));

				word.set(state + "@rent-own");
				output.set(rented + "/" + owned);
				context.write(word, output);
				
				/*************************************
				 * Q(4) Urban vs. rural
				 *************************************/
				urbanInside = Integer.parseInt(line.substring(1857, 1866));
				urbanOutside = Integer.parseInt(line.substring(1866, 1875));
				rural = Integer.parseInt(line.substring(1875, 1884));

				word.set(state + "@rural-urban");
				output.set(rural + "/" + (urbanInside + urbanOutside));
				context.write(word, output);
				
				/*************************************
				 * Q(5) Median house value (owner occupied)
				 *************************************/
				
				
			}

		}// END While loop

	}// END map

}
