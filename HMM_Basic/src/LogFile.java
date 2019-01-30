import java.util.ArrayList;
import java.util.Date;

//Class used to store one single log file loaded from the disk and the three different columns it contains. 
//After the timestamps, devIDs and messages have been loaded, they can no longer be modified in the class. 

public class LogFile 
{
	public ArrayList<Date> timestamps = new ArrayList<Date>();
	public ArrayList<Integer> devIDs = new ArrayList<Integer>();
	public ArrayList<String> messages = new ArrayList<String>();
	
	public LogFile(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn)
	{
		timestamps = timestampsIn;
		devIDs = devIdsIn;
		messages = messagesIn;
	}
	

}
