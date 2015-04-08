/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CensusPartitioner extends Partitioner<LongWritable, Text> {

	@Override
	public int getPartition(LongWritable key, Text value, int numReduceTasks) {
		
		//TODO implement
		return 0;
		
	}
	
}
