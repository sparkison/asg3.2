package cs455.util;

public class RangeBuilder {

	// Singleton instance
	private static RangeBuilder instance = null;

	// Exists only to defeat instantiation
	protected RangeBuilder() {

	}

	// Get instance of RangeBuilder
	public static RangeBuilder getInstance() {
		if (instance == null) {
			instance = new RangeBuilder();
		}
		return instance;
	}

	// Helper methods
	public String[] getHouseValueRanges(){
		String[] houseVals = new String[20];
		houseVals[0] = "Less than $15,000";
		houseVals[1] = "$15,000 - $19,999";
		houseVals[2] = "$20,000 - $24,999";
		houseVals[3] = "$25,000 - $29,999";
		houseVals[4] = "$30,000 - $34,999";
		houseVals[5] = "$35,000 - $39,999";
		houseVals[6] = "$40,000 - $44,999";
		houseVals[7] = "$45,000 - $49,999";
		houseVals[8] = "$50,000 - $59,999";
		houseVals[9] = "$60,000 - $74,999";
		houseVals[10] = "$75,000 - $99,999";
		houseVals[11] = "$100,000 - $124,999";
		houseVals[12] = "$125,000 - $149,999";
		houseVals[13] = "$150,000 - $174,999";
		houseVals[14] = "$175,000 - $199,999";
		houseVals[15] = "$200,000 - $249,999";
		houseVals[16] = "$250,000 - $299,999";
		houseVals[17] = "$300,000 - $399,999";
		houseVals[18] = "$400,000 - $499,999";
		houseVals[19] = "$500,000 or more";
		return houseVals;
	}

	public String[] getHouseRentRanges(){
		String[] rentVals = new String[17];
		rentVals[0] = "Less than $100";
		rentVals[1] = "$100 - $149";
		rentVals[2] = "$150 - $199";
		rentVals[3] = "$200 - $249";
		rentVals[4] = "$250 - $299";
		rentVals[5] = "$300 - $349";
		rentVals[6] = "$350 - $399";
		rentVals[7] = "$400 - $449";
		rentVals[8] = "$450 - $499";
		rentVals[9] = "$500 - $549";
		rentVals[10] = "$550 - $599";
		rentVals[11] = "$600 - $649";
		rentVals[12] = "$650 - $699";
		rentVals[13] = "$700 - $749";
		rentVals[14] = "$750 - $999";
		rentVals[15] = "$1000 or more";
		rentVals[16] = "No cash rent";
		return rentVals;
	}

	public String[] getRoomValueRange(){
		String[] numRooms = new String[9];
		numRooms[0] = "1 room";
		numRooms[1] = "2 rooms";
		numRooms[2] = "3 rooms";
		numRooms[3] = "4 rooms";
		numRooms[4] = "5 rooms";
		numRooms[5] = "6 rooms";
		numRooms[6] = "7 rooms";
		numRooms[7] = "8 rooms";
		numRooms[8] = "9 rooms";
		return numRooms;
	}

}
