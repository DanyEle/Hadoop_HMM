package it.cnr.isti.pad;
//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

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
	
	private static Date dateTimestamp = null;

	
	
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
	public void write(DataOutput out) throws IOException 
	{
		System.out.println("Writing fields of " + this.filePath);
		out.writeBytes(this.filePath + "\n");
		System.out.println("Size of arraylist is " + this.messages.size());
		
		//this size is used for messages, seqIDs, devIDs, timestamp.
		out.writeInt(this.messages.size());		
		
		//Serialize messages
		for(int i = 0; i < this.messages.size(); i++)
		{
			out.writeUTF(this.messages.get(i));
		}
		
		//serialize devIDs
		for(int i = 0; i < this.devIDs.size(); i++)
		{
			out.writeInt(this.devIDs.get(i));
		}
		
		//serialize sequenceIDs
		for(int i = 0; i < this.sequenceIDs.size(); i++)
		{
			out.writeInt(this.sequenceIDs.get(i));
		}	
		
		//serialize timestamps as strings
		for(int i = 0; i < this.timestamps.size(); i++)
		{
			out.writeUTF(this.timestamps.get(i).toString());
		}
	}
	
	public String toString()
	{
		String output ="";
		
		output += filePath;
		
		output += this.messages.toString();
		
		output += this.devIDs.toString();
		
		output += this.sequenceIDs.toString();
		
		output += this.timestamps.toString();
		
		return output;
	}
	
	

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		this.filePath = in.readLine();
		
		System.out.println("Read file path is " + this.filePath);
		

		int arrayListLength = in.readInt();
		
		//de-serialize messages
		for(int i = 0; i < arrayListLength; i++)
		{
			this.messages.add(in.readUTF());
			//System.out.println("Loaded " + loadedString);
		}
		//de-serialize dev IDs
		for(int i = 0; i < arrayListLength; i++)
		{
			this.devIDs.add(in.readInt());
		}	
		//de-serialize sequence IDs
		for(int i = 0; i < arrayListLength; i++)
		{
			this.sequenceIDs.add(in.readInt());
		}	
		//de-serialize timestamps
		for(int i = 0; i < arrayListLength; i++)
		{
			//used for parsing the serialized date.
			DateFormat dateFormatParse = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);			
			try 
			{
				dateTimestamp = dateFormatParse.parse(in.readUTF());
				//get back to the original formatting of the date
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				dateFormat.format(dateTimestamp);
				this.timestamps.add(dateTimestamp);
			} 
			catch (ParseException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
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
