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
import cs455.partition.CensusPartitioner;
import cs455.reduce.CensusReducer;

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
	 * This is the job for the Census versus analysis
	 * It is used for the Questions related to comparisons (this vs. that)
	 */
	public int start() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);

		// Remove old output path, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(conf, "Census versus analysis");
		job.setJarByClass(CensusDataJob.class);

		// Set Map, Partition, Combiner, and Reducer classes
		job.setMapperClass(CensusMapper.class);
		job.setPartitionerClass(CensusPartitioner.class);
		job.setNumReduceTasks(8);
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
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
