/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

//		// Remove old output paths, if exist
//		if (fs.exists(outPath)) {
//			fs.delete(outPath, true);
//		}
		if (fs.exists(outPath2)) {
			fs.delete(outPath2, true);
		}
//
//		Job job = Job.getInstance(conf, "Census analysis");
//		job.setJarByClass(CensusDataJob.class);
//
//		// Set Map, Partition, Combiner, and Reducer classes
//		job.setMapperClass(CensusMapper.class);
//		job.setPartitionerClass(CensusPartitioner.class);
//		job.setNumReduceTasks(7);
//		job.setReducerClass(CensusReducer.class);
//
//		// Set the Map output types
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		// Set the Reduce output types
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		// Set the output paths for the job
//		FileInputFormat.addInputPath(job, inPath);
//		FileOutputFormat.setOutputPath(job, outPath);
//
//		// Block for job to complete...
//		job.waitForCompletion(true);
		
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
		
		return job2.waitForCompletion(true) ? 0 : 1;
		
	}
	
}
