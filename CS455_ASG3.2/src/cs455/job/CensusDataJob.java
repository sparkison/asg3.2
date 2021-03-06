/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.map.CensusMapper;
import cs455.map.SecondaryMapper;
import cs455.partition.CensusPartitioner;
import cs455.reduce.CensusReducer;
import cs455.reduce.SecondaryReducer;

public class CensusDataJob {

	public static final Map<String, String> STATE_MAP;
	static {
		STATE_MAP = new HashMap<String, String>();
		STATE_MAP.put("AL", "Alabama");
		STATE_MAP.put("AK", "Alaska");
		STATE_MAP.put("AB", "Alberta");
		STATE_MAP.put("AZ", "Arizona");
		STATE_MAP.put("AR", "Arkansas");
		STATE_MAP.put("BC", "British Columbia");
		STATE_MAP.put("CA", "California");
		STATE_MAP.put("CO", "Colorado");
		STATE_MAP.put("CT", "Connecticut");
		STATE_MAP.put("DE", "Delaware");
		STATE_MAP.put("DC", "District Of Columbia");
		STATE_MAP.put("FL", "Florida");
		STATE_MAP.put("GA", "Georgia");
		STATE_MAP.put("GU", "Guam");
		STATE_MAP.put("HI", "Hawaii");
		STATE_MAP.put("ID", "Idaho");
		STATE_MAP.put("IL", "Illinois");
		STATE_MAP.put("IN", "Indiana");
		STATE_MAP.put("IA", "Iowa");
		STATE_MAP.put("KS", "Kansas");
		STATE_MAP.put("KY", "Kentucky");
		STATE_MAP.put("LA", "Louisiana");
		STATE_MAP.put("ME", "Maine");
		STATE_MAP.put("MB", "Manitoba");
		STATE_MAP.put("MD", "Maryland");
		STATE_MAP.put("MA", "Massachusetts");
		STATE_MAP.put("MI", "Michigan");
		STATE_MAP.put("MN", "Minnesota");
		STATE_MAP.put("MS", "Mississippi");
		STATE_MAP.put("MO", "Missouri");
		STATE_MAP.put("MT", "Montana");
		STATE_MAP.put("NE", "Nebraska");
		STATE_MAP.put("NV", "Nevada");
		STATE_MAP.put("NB", "New Brunswick");
		STATE_MAP.put("NH", "New Hampshire");
		STATE_MAP.put("NJ", "New Jersey");
		STATE_MAP.put("NM", "New Mexico");
		STATE_MAP.put("NY", "New York");
		STATE_MAP.put("NF", "Newfoundland");
		STATE_MAP.put("NC", "North Carolina");
		STATE_MAP.put("ND", "North Dakota");
		STATE_MAP.put("NT", "Northwest Territories");
		STATE_MAP.put("NS", "Nova Scotia");
		STATE_MAP.put("NU", "Nunavut");
		STATE_MAP.put("OH", "Ohio");
		STATE_MAP.put("OK", "Oklahoma");
		STATE_MAP.put("ON", "Ontario");
		STATE_MAP.put("OR", "Oregon");
		STATE_MAP.put("PA", "Pennsylvania");
		STATE_MAP.put("PE", "Prince Edward Island");
		STATE_MAP.put("PR", "Puerto Rico");
		STATE_MAP.put("QC", "Quebec");
		STATE_MAP.put("RI", "Rhode Island");
		STATE_MAP.put("SK", "Saskatchewan");
		STATE_MAP.put("SC", "South Carolina");
		STATE_MAP.put("SD", "South Dakota");
		STATE_MAP.put("TN", "Tennessee");
		STATE_MAP.put("TX", "Texas");
		STATE_MAP.put("UT", "Utah");
		STATE_MAP.put("VT", "Vermont");
		STATE_MAP.put("VI", "Virgin Islands");
		STATE_MAP.put("VA", "Virginia");
		STATE_MAP.put("WA", "Washington");
		STATE_MAP.put("WV", "West Virginia");
		STATE_MAP.put("WI", "Wisconsin");
		STATE_MAP.put("WY", "Wyoming");
		STATE_MAP.put("YT", "Yukon Territory");
	}

	private String inputPath;
	private String outputPath;

	public CensusDataJob(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	public String getInputPath(){
		return new String(inputPath);
	}

	public String getOutputPath(){
		return new String(outputPath);
	}

	/*
	 * This is the job for the Census analysis
	 */
	public int start(int level) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);
		Path outPath2 = new Path(outputPath + "_2");
		int status = -1;

		// If level 3, do it all
		if (level == 3) {

			// Remove old output paths, if exist
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}

			Job job = Job.getInstance(conf, "Census analysis");
			job.setJarByClass(CensusDataJob.class);

			// Set Map, Partition, Combiner, and Reducer classes
			job.setMapperClass(CensusMapper.class);
			job.setPartitionerClass(CensusPartitioner.class);
			job.setNumReduceTasks(7);
			job.setReducerClass(CensusReducer.class);

			// Set the Map output types
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// Set the Reduce output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Set the output paths for the job
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);

			// Block for job to complete...
			job.waitForCompletion(true);
			
			// Set level == 2 to run next task
			level = 2;
			
		}

		// If level 2, process secondary output (the output of the first MR job)
		if (level == 2) {

			// Remove old output paths, if exist
			if (fs.exists(outPath2)) {
				fs.delete(outPath2, true);
			}

			/*
			 * Start MR stage 2 for computing values for Q7 and Q8
			 */

			Configuration conf2 = new Configuration();

			Job job2 = Job.getInstance(conf2, "Census analysis stage 2");
			job2.setJarByClass(CensusDataJob.class);

			// Set Map, Partition, Combiner, and Reducer classes
			job2.setMapperClass(SecondaryMapper.class);
			job2.setReducerClass(SecondaryReducer.class);

			// Set the Map output types
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			// Set the Reduce output types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// Set the output paths for the job
			FileInputFormat.addInputPath(job2, new Path(outputPath + "/part-r-00006")); // This is the results of Q7 and Q8 from stage 1
			FileOutputFormat.setOutputPath(job2, outPath2);

			// Wait for job to complete
			status = job2.waitForCompletion(true) ? 0 : 1;
		}

		// If level 1, just format output
		if (level == 1) {
			status = 0;
		}


		/*
		 * Process results into more readable form
		 * 
		 * The below code simply parses the MR output
		 * to a more human-readable form.
		 * 
		 * It is not needed to decipher the results, only to
		 * combine and simplify them
		 */
		if (status == 0) {

			System.out.println("****************************************************************************");
			System.out.println("	MapReduce tasks completed successfully! Formatting results...");
			System.out.println("****************************************************************************\n");

			try{
				// Save results
				PrintWriter pw = new PrintWriter("Results");
				List<String> qList = new ArrayList<String>();

				/**********************
				 * BEGIN Q1 formatting
				 **********************/
				Path pt = new Path(outPath.toString() + "/part-r-00000");
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q1");
				pw.println("****************************************************************************");

				for (int i = 0; i<qList.size()-1; i+=2) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");
					String[] split2 = qList.get(i+1).split("\t");
					String result = "For the state of " + state + ",\t" + split[1].split("=")[1].trim() + "\tresidence rented while " + split2[1].split("=")[1].trim() + "\tresidence owned.";
					pw.println(result);
					// System.out.println(result);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q2 formatting
				 **********************/
				pt = new Path(outPath.toString() + "/part-r-00001");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q2");
				pw.println("****************************************************************************");

				for (int i = 0; i<qList.size()-1; i+=2) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");
					String[] split2 = qList.get(i+1).split("\t");
					String result = "For the state of " + state + ",\t" + split[1].split("=")[1].trim() + "\tof males never married and " + split2[1].split("=")[1].trim() + "\tof females never married.";
					pw.println(result);
					// System.out.println(result);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q3(a,b and c) formatting
				 **********************/
				pt = new Path(outPath.toString() + "/part-r-00002");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q3");
				pw.println("****************************************************************************");

				/*
				 	Input format:
				  	AK % Male 18 and under (of total pop)	3646/550043 = 0.6628573%	(split)
					AK % Female 18 and under (of total pop)	3383/550043 = 0.6150428%	(split2)
					AK % Male age 19 to 29 (of total pop)	2491/550043 = 0.45287368%	(split3)
					AK % Female age 19 to 29 (of total pop)	1746/550043 = 0.31742972%	(split4)
					AK % Male age 30 to 39 (of total pop)	1871/550043 = 0.3401552%	(split5)
					AK % Female age 30 to 39 (of total pop)	1615/550043 = 0.2936134%	(split6)
				 */

				for (int i = 0; i<qList.size()-5; i+=6) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");
					String[] split2 = qList.get(i+1).split("\t");
					String[] split3 = qList.get(i+2).split("\t");
					String[] split4 = qList.get(i+3).split("\t");
					String[] split5 = qList.get(i+4).split("\t");
					String[] split6 = qList.get(i+5).split("\t");

					String result = "For the state of " + state + ",\t" + split[1].split("=")[1].trim() 
							+ "\tof males are 18 and under, " + split3[1].split("=")[1].trim() 
							+ "\tof males are 19 to 29, and " + split5[1].split("=")[1].trim() 
							+ "\tof males are 30 to 39.";

					String resul2 = "For the state of " + state + ",\t" + split2[1].split("=")[1].trim() 
							+ "\tof females are 18 and under, " + split4[1].split("=")[1].trim() 
							+ "\tof females are 19 to 29, and " + split6[1].split("=")[1].trim() 
							+ "\tof females are 30 to 39.";

					pw.println(result);
					pw.println(resul2);
					// System.out.println(result);
					// System.out.println(result2);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q4 formatting
				 **********************/
				pt = new Path(outPath.toString() + "/part-r-00003");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q4");
				pw.println("****************************************************************************");

				for (int i = 0; i<qList.size()-1; i+=2) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");
					String[] split2 = qList.get(i+1).split("\t");

					String result = "For the state of " + state + ",\t" + split[1].split("=")[1].trim() + "\tof households are Rural and " + split2[1].split("=")[1].trim() + "\tof households are Urban.";

					pw.println(result);
					// System.out.println(result);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q5 formatting
				 **********************/
				pt = new Path(outPath.toString() + "/part-r-00004");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q5");
				pw.println("****************************************************************************");

				for (int i = 0; i<qList.size(); i++) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");

					String result = "For the state of " + state + ",\tthe median value of house occupied by owner is " + split[1];

					pw.println(result);
					// System.out.println(result);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q6 formatting
				 **********************/
				pt = new Path(outPath.toString() + "/part-r-00005");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q6");
				pw.println("****************************************************************************");

				for (int i = 0; i<qList.size(); i++) {

					String state = STATE_MAP.get(qList.get(i).substring(0, 2));
					String[] split = qList.get(i).split("\t");

					String result = "For the state of " + state + ",\tthe median rent paid is " + split[1];

					pw.println(result);
					// System.out.println(result);
				}
				pw.println();
				System.out.println();
				qList.clear();

				/**********************
				 * BEGIN Q7 and Q8 formatting
				 **********************/
				pt = new Path(outPath2.toString() + "/part-r-00000");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					qList.add(line);
					line = br.readLine();
				}
				br.close();

				pw.println("****************************************************************************");
				pw.println("	Results for Q7 and Q8");
				pw.println("****************************************************************************");

				String[] split = qList.get(0).split("\t");
				String[] split2 = qList.get(1).split("\t");

				String result = split[0] + " " + split[1] + "\n" + split2[0] + " " + split2[1];

				pw.println(result);
				// System.out.println(result);

				System.out.println();

				// Close print writer, clear list
				pw.flush();    
				pw.close();
				
			}catch(Exception e){
				System.out.println("Error formatting output.");
				System.out.println(e.getMessage());
			}
		}// END Results processing

		return status;

	}

}