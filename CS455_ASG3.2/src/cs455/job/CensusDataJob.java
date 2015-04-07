package cs455.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cs455.map.CensusDataGrabber;
import cs455.reduce.CensusDataReducer;

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
	
	public void start() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);

		// Remove old output path, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(conf, "Giga-sort job");
		job.setJarByClass(CensusDataJob.class);

		// Set Map, Partition, Combiner, and Reducer classes
		job.setMapperClass(CensusDataGrabber.class);
		job.setReducerClass(CensusDataReducer.class);

		// Set the Map output types
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Set the Reduce output types
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the output paths for the job
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		// Block for job to complete...
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
