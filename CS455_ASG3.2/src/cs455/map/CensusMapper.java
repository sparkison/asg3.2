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

import cs455.util.RangeBuilder;

/*
 * Output formats: 
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
		int urbanInside, urbanOutside, rural, notDefined;

		// Split on newline
		StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
		// Iterate through each line and process
		while(lineItr.hasMoreTokens()){

			/*
			 * ***************************************************
			 * Primary record information
			 * ***************************************************
			 */

			String line = lineItr.nextToken();
			state = line.substring(8, 10);
			summaryLevel = Integer.parseInt(line.substring(10, 13));

			// Only analyze at summary level 100 
			if (summaryLevel != MAX_LEVEL)
				continue;

			/*
			 * Get record information
			 * In case of malformed line, continue to next
			 * line. Prevents malformed file from breaking
			 * calculation job.
			 */
			logicalRecordPart = 0;
			try {
				logicalRecord = Integer.parseInt(line.substring(18, 24));
				logicalRecordPart = Integer.parseInt(line.substring(24, 28));
				totalRecordParts = Integer.parseInt(line.substring(28, 32));
			} catch (NumberFormatException e) {
				continue;
			}

			// Sanity check; used for debugging
			//			String result = "State:\t" + state + 
			//					", Summary level:\t" + summaryLevel + 
			//					", Logical Record:\t" + logicalRecord + 
			//					", Record Part:\t" + logicalRecordPart + 
			//					", Total Record Parts:\t" + totalRecordParts;


			/*
			 * ***************************************************
			 * END Primary record information
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
				output.set(maleUnmarried + "/" + femaleUnmarried + "/" + malePop + "/" + femalePop);
				context.write(word, output);

				/*************************************
				 * Q(3a) Male 18 and under/female 18 and under
				 *************************************/
				int maleUnder18 = 0;
				int femaleUnder18 = 0;
				int start;
				/*
				 * Males aged 18 and under
				 * Start index = 3864, end = 3981
				 * (3981-3864)/9 = 13
				 */
				start = 3864;
				for (int i = 0; i<13; i++) {
					maleUnder18 += Integer.parseInt(line.substring(start, start + 9));
					start += 9;
				}
				/*
				 * Females aged 18 and under
				 * Start index = 4143, end = 4260
				 * (4260-4143)/9 = 13
				 */
				start = 4143;
				for (int i = 0; i<13; i++) {
					femaleUnder18 += Integer.parseInt(line.substring(start, start + 9));
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
					male19to29 += Integer.parseInt(line.substring(start, start + 9));
					start += 9;
				}
				/*
				 * Females aged 19 to 29
				 * Start index = 4260, end = 4305
				 * (4305-4260)/9 = 5
				 */
				start = 4260;
				for (int i = 0; i<5; i++) {
					female19to29 += Integer.parseInt(line.substring(start, start + 9));
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
					male30to39 += Integer.parseInt(line.substring(start, start + 9));
					start += 9;
				}
				/*
				 * Females aged 30 to 39
				 * Start index = 4305, end = 4323
				 * (4323-4305)/9 = 2
				 */
				start = 4305;
				for (int i = 0; i<2; i++) {
					female30to39 += Integer.parseInt(line.substring(start, start + 9));
					start += 9;
				}
				word.set(state + "@male30to39-female30to39");
				output.set(male30to39 + "/" + female30to39 + "/" + totalPop);
				context.write(word, output);
				
				/*************************************
				 * Q(8) Population over 85
				 *************************************/
				int maleOver85 = 0;
				int femalOver85 = 0;				
				/*
				 * Males aged 85 and older
				 * Start index = 4134
				 */
				maleOver85 = Integer.parseInt(line.substring(4134, 4143));
				/*
				 * Females aged 85 and older
				 * Start index = 4413
				 */
				femalOver85 = Integer.parseInt(line.substring(4413, 4422));
				word.set(state + "@maleOver85-femalOver85");
				output.set((maleOver85 + femalOver85) + "/" + totalPop);
				context.write(word, output);

			}
			
			if (logicalRecordPart == 2) {
				
				RangeBuilder rb = RangeBuilder.getInstance();
				
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
				notDefined = Integer.parseInt(line.substring(1884, 1893));

				word.set(state + "@rural-urban");
				output.set(rural + "/" + (urbanInside + urbanOutside) + "/" + notDefined);
				context.write(word, output);

				/*************************************
				 * Q(5) Median house value (owner occupied)
				 *************************************/
				int start;
				int homeValue;
				String[] houseVals = rb.getHouseValueRanges();
				word.set(state + "@home-value");
				/*
				 * Start index = 2928, end = 3108
				 * (3108-2928)/9 = 20
				 */
				start = 2928;
				for (int i = 0; i<20; i++) {
					homeValue = Integer.parseInt(line.substring(start, start + 9));
					output.set(houseVals[i] + "=" + homeValue);
					context.write(word, output);
					start += 9;
				}

				/*************************************
				 * Q(6) Median rent paid
				 *************************************/
				houseVals = rb.getHouseRentRanges();
				word.set(state + "@rent-value");
				/*
				 * Start index = 3450, end = 3603
				 * (3603-3450)/9 = 17
				 */
				start = 3450;
				for (int i = 0; i<17; i++) {
					homeValue = Integer.parseInt(line.substring(start, start + 9));
					output.set(houseVals[i] + "=" + homeValue);
					context.write(word, output);
					start += 9;
				}

				/*************************************
				 * Q(7) Avg. number of rooms
				 *************************************/
				String[] numRooms = rb.getRoomValueRange();
				word.set(state + "@number-rooms");
				int roomCount;
				/*
				 * Start index = 2388, end = 2469
				 * (2469-2388)/9 = 9
				 */
				start = 2388;
				for (int i = 0; i<9; i++) {
					roomCount = Integer.parseInt(line.substring(start, start + 9));
					output.set(numRooms[i] + "=" + roomCount);
					context.write(word, output);
					start += 9;
				}
				
			}

		}// END While loop

	}// END map
	
}
