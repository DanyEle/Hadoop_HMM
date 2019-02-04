//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

//Class used to store one single log file loaded from the disk and the three different columns it contains along with the
//sequence ID corresponding to each one of the messages loaded. 

public class LogFile 
{
	public ArrayList<Date> timestamps = new ArrayList<Date>();
	public ArrayList<Integer> devIDs = new ArrayList<Integer>();
	public ArrayList<String> messages = new ArrayList<String>();
	public ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
	
	
	
	public LogFile()
	{
		
	}
	
	public LogFile(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn)
	{
		this.timestamps = timestampsIn;
		this.devIDs = devIdsIn;
		this.messages = messagesIn;
	}
	
	public LogFile(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn, ArrayList<Integer> sequenceIDsIn)
	{
		this.timestamps = timestampsIn;
		this.devIDs = devIdsIn;
		this.messages = messagesIn;
		this.sequenceIDs = sequenceIDsIn;
	}
	
	public void setMessages(ArrayList<String> messagesIn)
	{
		messages = messagesIn;		
	}
	
	public void setDevIDs(ArrayList<Integer> devIDsIn)
	{
		devIDs = devIDsIn;		
	}
	
	public void setTimeStamps(ArrayList<Date> timestampsIn)
	{
		timestamps = timestampsIn;		
	}
	
	public void setSequenceID(ArrayList<Integer> sequenceIDsIn)
	{
		sequenceIDs = sequenceIDsIn;		
	}
	
	
	
	
	
	

}
