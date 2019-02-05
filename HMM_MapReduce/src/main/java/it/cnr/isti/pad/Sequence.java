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
public class Sequence implements WritableComparable<Sequence>, Serializable
{

	public String filePath;
	
	public ArrayList<Date> timestamps = new ArrayList<Date>();
	public ArrayList<Integer> devIDs = new ArrayList<Integer>();
	public ArrayList<String> messages = new ArrayList<String>();
	public ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
	
	private static Date dateTimestamp = null;

	private static 	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	
	
	public Sequence()
	{
		
	}
	
	public Sequence(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn, String filePathIn)
	{
		this.timestamps = timestampsIn;
		this.devIDs = devIdsIn;
		this.messages = messagesIn;
		this.filePath = filePathIn;
	}
	
	public Sequence(ArrayList<Date> timestampsIn, ArrayList<Integer> devIdsIn, ArrayList<String>  messagesIn, 
			ArrayList<Integer> sequenceIDsIn, String filePathIn)
	{
		this.timestamps = timestampsIn;
		this.devIDs = devIdsIn;
		this.messages = messagesIn;
		this.sequenceIDs = sequenceIDsIn;
		this.filePath = filePathIn;
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
	
	
	@Override
	public void write(DataOutput out) throws IOException 
	{		
		System.out.println("Writing fields of sequence with ID: " + this.sequenceIDs.get(0));
		//serialize sequenceID
		out.writeInt(this.sequenceIDs.get(0));
		
		//serialize the devID
		out.writeInt(this.devIDs.get(0));
		
		//serialize filePath
		out.writeUTF(this.filePath + "\n");
		
		//serialize size of messagesArraylist and timestampsArraylist. 
		out.writeInt(this.messages.size());		
		
		//Serialize messages
		for(int i = 0; i < this.messages.size(); i++)
		{
			out.writeUTF(this.messages.get(i));
		}		
		//serialize timestamps as strings
		for(int i = 0; i < this.timestamps.size(); i++)
		{
			out.writeUTF(this.timestamps.get(i).toString());
		}
		
	}	
	
	
	public String toString()
	{
		//System.out.println("toString of sequence with ID: " + this.sequenceIDs.get(0));

		String out = "";
		
		//serialize sequenceID
		out += this.sequenceIDs.toString();
		
		//serialize the devID
		out += this.devIDs.toString();
		
		//serialize filePath
		out += this.filePath;
		
		//serialize size of messagesArraylist and timestampsArraylist. 
		out += this.messages.size();		
		
		//Serialize messages
		out += this.messages.toString();
		
		out += this.timestamps.toString();
		
		return out;
	}
	
	//big issue in readFields: at every step, we read the current sequences along with all the preceding ones. 
	

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		//firstly, clear all the existing fields
		this.devIDs.clear();
		this.filePath = "";
		this.sequenceIDs.clear();
		this.messages.clear();
		this.timestamps.clear();
		
		//deserialize sequenceID
		int sequenceID = in.readInt();
		
		System.out.println("Read fields of sequence with ID: " + sequenceID);
		
		//deserialize devID
		int devID = in.readInt();
		
		//deserialize filePath
		String filePath = in.readUTF();	
		
		this.filePath = filePath;

		//deserialize messagesArrayList and timestampsArrayList
		int arrayListLength = in.readInt();
		
		//populate sequenceIDs
		for(int i = 0; i < arrayListLength; i++)
		{
			this.sequenceIDs.add(sequenceID);
		}	
		
		//populate devIDs
		for(int i = 0; i < arrayListLength; i++)
		{
			this.devIDs.add(devID);
		}			
		
		//de-serialize messages
		for(int i = 0; i < arrayListLength; i++)
		{
			this.messages.add(in.readUTF());
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
	public int compareTo(Sequence logFileComp) {
		 int cmp = this.filePath.compareTo(logFileComp.filePath);
	        if (cmp != 0) {
	            return cmp;
	        }
	        return this.filePath.compareTo(logFileComp.filePath);
	}

	
	
	

}
