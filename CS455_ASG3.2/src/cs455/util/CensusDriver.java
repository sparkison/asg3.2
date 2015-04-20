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
		
		if(args.length < 3 || args == null) {
			System.out.println("Incorrect number of arguments used.\nPlease use: \"cs455.util.CensusDriver [input_path] [output_path] [level (3,2, or 1)]\"");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		int processType = Integer.parseInt(args[2]);
		int status = -1;
		
		// The census job runner
		CensusDataJob censusJob = new CensusDataJob(input, output);
		try {
			/*
			 * Need to set level
			 * 3 = Process primary census data, run secondary MR job on the results
			 * 2 = Only run the secondary MR job (assumes output from first job is present)
			 * 1 = Just format the results for the first and second MR jobs (assumes both outputs are present)
			 */
			status = censusJob.start(processType);
		} catch (IllegalArgumentException | ClassNotFoundException
				| IOException | InterruptedException e) {
			System.out.println("Error starting Census map reduce job: ");
			e.printStackTrace();
			System.exit(status);
		}
		
	}
	
}
