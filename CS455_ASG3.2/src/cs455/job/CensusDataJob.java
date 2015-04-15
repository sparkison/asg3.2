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
	public int start() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);
		Path outPath2 = new Path(outputPath + "_2");

		// Remove old output paths, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		if (fs.exists(outPath2)) {
			fs.delete(outPath2, true);
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
		int status = job2.waitForCompletion(true) ? 0 : 1;

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
			    PrintWriter pw = new PrintWriter("Census_Results");
			    
			    
			    
			    
				/*
				 * BEGIN Q1 formatting
				 */
				List<String> q1List = new ArrayList<String>();
				Path pt = new Path(outPath.toString() + "/part-r-00000");
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null){
					q1List.add(line);
					line = br.readLine();
				}
				br.close();
				
				pw.println("****************************************************************************");
				pw.println("	Results for Q1");
				pw.println("****************************************************************************");
				
				for (int i = 0; i<q1List.size()-1; i++) {
					
					String state = STATE_MAP.get(q1List.get(i).substring(0, 2));
					String[] split = q1List.get(i).split("\t");
					String[] split2 = q1List.get(i+1).split("\t");
					String result = "For the state of " + state + ", " + split[1].split("=")[1].trim() + " residence rented while " + split2[1].split("=")[1].trim() + " residence owned.";
					pw.println(result);
					// System.out.println(result);
				}
				System.out.println();
				
				
				
				
				/*
				 * BEGIN Q2 formatting
				 */
				List<String> q2List = new ArrayList<String>();
				pt = new Path(outPath.toString() + "/part-r-00001");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					q2List.add(line);
					line = br.readLine();
				}
				br.close();
				
				pw.println("****************************************************************************");
				pw.println("	Results for Q2");
				pw.println("****************************************************************************");
				
				for (int i = 0; i<q2List.size()-1; i++) {
					
					String state = STATE_MAP.get(q1List.get(i).substring(0, 2));
					String[] split = q1List.get(i).split("\t");
					String[] split2 = q1List.get(i+1).split("\t");
					String result = "For the state of " + state + ", " + split[1].split("=")[1].trim() + " of males never married and " + split2[1].split("=")[1].trim() + " of females never married.";
					pw.println(result);
					// System.out.println(result);
				}
				System.out.println();
				
				// Close print writer
				pw.flush();    
		        pw.close();
		        
		        
		        
		        
		        /*
				 * BEGIN Q3(a,b and c) formatting
				 */
				List<String> q3List = new ArrayList<String>();
				pt = new Path(outPath.toString() + "/part-r-00002");
				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				line = br.readLine();
				while (line != null){
					q3List.add(line);
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
				
				for (int i = 0; i<q3List.size()-6; i++) {
					
					String state = STATE_MAP.get(q1List.get(i).substring(0, 2));
					String[] split = q1List.get(i).split("\t");
					String[] split2 = q1List.get(i+1).split("\t");
					String[] split3 = q1List.get(i+2).split("\t");
					String[] split4 = q1List.get(i+3).split("\t");
					String[] split5 = q1List.get(i+4).split("\t");
					String[] split6 = q1List.get(i+5).split("\t");
					
					String result = "For the state of " + state + ", " + split[1].split("=")[1].trim() 
							+ " of males are 18 and under, " + split3[1].split("=")[1].trim() 
							+ " of males are 19 to 29, and " + split5[1].split("=")[1].trim() 
							+ " of males are 30 to 39.";
					
					String resul2 = "For the state of " + state + ", " + split2[1].split("=")[1].trim() 
							+ " of females are 18 and under, " + split4[1].split("=")[1].trim() 
							+ " of females are 19 to 29, and " + split6[1].split("=")[1].trim() 
							+ " of females are 30 to 39.";
					
					pw.println(result);
					pw.println(resul2);
					// System.out.println(result);
					// System.out.println(result2);
				}
				System.out.println();
				
				// Close print writer
				pw.flush();    
		        pw.close();
		        
		        
		        /*
				 * BEGIN Q4 formatting
				 */
		        
		        
			}catch(Exception e){}
		}// END Results processing
		
		return status;

	}

}
