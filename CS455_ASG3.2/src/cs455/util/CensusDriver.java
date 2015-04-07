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
			System.out.println("Incorrect number of argument used.\nPlease use: \"java CensusDriver [input_path] [output_path]\"");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		
		// The census job
		CensusDataJob censusJob = new CensusDataJob(input, output);
		try {
			// Used to find answer to Q1: per-state breakdown of rented vs. owned
			censusJob.q1();
		} catch (IllegalArgumentException | ClassNotFoundException
				| IOException | InterruptedException e) {
			System.out.println("Error starting Census map reduce job: ");
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
}
