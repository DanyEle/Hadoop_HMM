package it.cnr.isti.pad;
//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;


import org.apache.hadoop.io.WritableComparable;

//Class used to store one single log file loaded from the disk and the three different columns it contains along with the
//sequence ID corresponding to each one of the messages loaded. 

@SuppressWarnings("serial")
public class LogFile implements WritableComparable<LogFile>, Serializable
{
	

	public String filePath;
	
	public ArrayList<Date> timestamps = new ArrayList<Date>();
	public ArrayList<Integer> devIDs = new ArrayList<Integer>();
	public ArrayList<String> messages = new ArrayList<String>();
	public ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
	
	public LogFile()
	{
		
	}
	
	public LogFile(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn, String filePathIn)
	{
		this.timestamps = timestampsIn;
		this.devIDs = devIdsIn;
		this.messages = messagesIn;
		this.filePath = filePathIn;
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
	
	//Serialization code underneath

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		int devIDsSize = in.readInt();
		//System.out.println("Reading arraylist size of " + devIDsSize);
		
		this.devIDs = new ArrayList<Integer>(devIDsSize);
		
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		//System.out.println("Writing arraylist size of " + this.devIDs.size());
		out.writeInt(this.devIDs.size());		
		
	}

	@Override
	public int compareTo(LogFile logFileComp) {
		 int cmp = this.filePath.compareTo(logFileComp.filePath);
	        if (cmp != 0) {
	            return cmp;
	        }
	        return this.filePath.compareTo(logFileComp.filePath);
	}

	
	

}
