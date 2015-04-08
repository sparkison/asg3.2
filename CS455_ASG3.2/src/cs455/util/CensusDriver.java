/**
 * @author Shaun Parkison (shaunpa)
 * CS455 - ASG3
 * Census data analysis using MapReduce
 */

package cs455.util;

import java.io.IOException;

import cs455.job.CensusDataJob;

public class CensusDriver {

	public static void main(String args[]) {
		
		if(args == null || args.length < 1) {
			System.out.println("Incorrect number of arguments used.\nPlease use: \"cs455.util.CensusDriver [input_path] [output_path]\"");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		int status;
		
		// The census job runner
		CensusDataJob censusJob = new CensusDataJob(input, output);
		try {
			status = censusJob.start();
		} catch (IllegalArgumentException | ClassNotFoundException
				| IOException | InterruptedException e) {
			System.out.println("Error starting Census map reduce job: ");
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
}
