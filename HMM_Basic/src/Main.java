//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

	public static void main(String[] args) throws FileNotFoundException, ParseException, IOException {
		
		
		//messagesFilter represent outlier symbols to remove. 
		ArrayList<String> messagesRemove = new ArrayList<>(Arrays.asList("View.OnChangeCaretLine", "View.OnChangeScrollInfo", "View.File", 
				"Debug.Debug Break Mode", "Debug.Debug Run Mode","Debug.DebugType", "Debug.Enter Design Mode","Build.BuildDone", "Build.BuildBegin"));
		
		ArrayList<LogFile> loadedLogFiles = new ArrayList<LogFile>();

		LogFile logFileFiltered1 = loadFilterSingleDataset("/home/hadoop/ABB_Logs/BlazeData_Dev_233.csv", messagesRemove, 1);
		//LogFile logFileFiltered2 = loadFilterSingleDataset("/home/hadoop/ABB_Logs_Small/BlazeData_Dev_35.csv", messagesRemove, 10000000);
		
		loadedLogFiles.add(logFileFiltered1);
		//loadedLogFiles.add(logFileFiltered2);
		
		//System.out.println(logFileFiltered1.toString());
		
		//need to merge the log files by appending the respective array lists' contents one after the other.
		LogFile reducedLogFile = reduceArrayLists(loadedLogFiles);
		
		return;
	}
	
	//Input: - String filePath: the path of a file to be loaded and filtered. 
	//		 - ArrayList<String> messagesRemove: a set of messages that ought to be remove from the dataset.
	//		 - int startIndex: the starting index of the sequenceIrD for the current file. 
	//Output: the file with three columns and a sequenceID assigned to contiguous sequences.
	public static LogFile loadFilterSingleDataset(String filePath, ArrayList<String> messagesRemove, int startIndex) throws FileNotFoundException, ParseException, IOException
	{
		System.out.println("Loading data from " + filePath);
		
		//we have successfully loaded the file from the disk into three array lists
		LogFile logFile = readArraysFromFileContent(filePath);
		System.out.println("Amount of messages before removing outliers: " + logFile.messages.size());
		
		//we have successfully remove all those lines that contain an outlier message.
		LogFile logFileNoOutliers = removeOutliers(logFile, messagesRemove);
		
		System.out.println("Amount of messages after removing outliers: " + logFileNoOutliers.messages.size());
		
		//we have successfully marked every single message with a sequence ID and remove those messages that do not belong to any sequence. 
		LogFile logFileSeqID = markSequenceIDs(logFileNoOutliers, startIndex);
		
		//let's just try to display all the sequenceIDs assigned and get the amount of messages remaining
		System.out.println("Amount of messages after marking sequence IDs (and removing the ones not in sequences): " + logFileSeqID.sequenceIDs.size());
		
		//now remove all the sequences that are too small (< 2 minutes in length) and large sessions (> 100 minutes in length)
		//LogFile logFileFiltered = removeLongSmallSequences(logFileSeqID);
		
		//System.out.println("Amount of messages after after removing long and small sequences:" + logFileFiltered.sequenceIDs.size());
		
		return logFileSeqID;
	}
	

	//Input: the path of a file containing three columns: timestamp, dev_id, message
	//Output: an object storing each one of the three different columns in array lists for easy access. 
	public static LogFile readArraysFromFileContent(String filePath) throws ParseException, FileNotFoundException, IOException
	{
		ArrayList<Date> timestamps = new ArrayList<Date>();
		ArrayList<Integer> devIDs = new ArrayList<Integer>();
		ArrayList<String> messages = new ArrayList<String>();

        String line = "";
        String separator = ",";
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) 
        {
        	//skip the first line, which contains the header.
        	br.readLine();
        	
            while ((line = br.readLine()) != null) 
            {
                String[] allColumns = line.split(separator);
                
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                Date parsedTimestamp = dateFormat.parse(allColumns[0]);     
                int devId = new Integer(allColumns[1]);
                String message = new String(allColumns[2]);
                //add the timestamp, the dev_id and the message to the respective array lists
                timestamps.add(parsedTimestamp);
                devIDs.add(devId);
                messages.add(message);                
                         
               // System.out.println("Timestamp = " + allColumns[0] + " , dev_id=" + allColumns[1] + ", message = " + allColumns[2]);
            }
        } 
        
        //all data columns loaded into respective array lists, fine. Use an object to encapsulate them.
        LogFile logFileLoaded = new LogFile(timestamps, devIDs, messages);
        
        return logFileLoaded;
	}
	
	//Input: a LogFile object and a set of messages to be remove from the logFile
	//Output: a LogFile with the outliers removed. 
	public static LogFile removeOutliers(LogFile logFile, ArrayList<String> messagesRemove)
	{
		
		ArrayList<Date> timestamps = new ArrayList<Date>();
		ArrayList<Integer> devIDs = new ArrayList<Integer>();
		ArrayList<String> messages = new ArrayList<String>();
		
		//now iterate through all the logs' lines and remove all those lines 
		//that contain a message which is found to be an outlier. 
		for(int i = 0; i < logFile.messages.size(); i++)
		{
			//firstly, identify whether the current message is an outlier (i.e: in the messagesRemove)
			//and in that case removes the corresponding timestamp and dev ID as well. 
			String message = logFile.messages.get(i);
			
			//if not an outlier, then add it. we perform some filtering here. 
			if(!(messagesRemove.contains(message)))
			{
				devIDs.add(logFile.devIDs.get(i));
				messages.add(logFile.messages.get(i));
				timestamps.add(logFile.timestamps.get(i));
			}
		}
		
		logFile.setDevIDs(devIDs);
		logFile.setTimeStamps(timestamps);
		logFile.setMessages(messages);
		
		return logFile;		
	}
	
	//Input: - logFile: a logFile with timestamps, devIDs and messages
	//		 - startIndex: the starting index of the sequence ID to be assigned to messages. 
	//Output: a logFile with one sequence ID assigned to every single message, and messages assigned to no sequences removed. 
	public static LogFile markSequenceIDs(LogFile logFile, int startIndex)
	{
		
		//these are the IDE debug messages, which act as a starting point for the interaction of a developer with the IDE
		ArrayList<String> messagesDebug = new ArrayList<>(Arrays.asList(
				"Debug.ToggleBreakpoint", "Debug.CallStack", "View.Call Stack", "Debug.Start", "Debug.StepOver", "Debug.StepInto", "Debug.StepOut", 
                "Debug.AttachtoProcess", "Debug.StartDebugTarget", "Debug.StopDebugging", "Debug.QuickWatch", "Debug.AddWatch", "View.Watch 1", 
                "Debug.DisableAllBreakpoints", "Debug.DetachAll", "Debug.Restart", "Debug.RunToCursor", "Debug.EnableAllBreakpoints", 
                "Debug.ShowNextStatement", "Debug.BreakatFunction", "Debug.StartPerformanceAnalysis", "Debug.AddParallelWatch", 
                "Debug.Threads", "Debug.Disassembly", "Debug.GoToDisassembly", "Debug.EvaluateStatement", "Debug.SetNextStatement", 
                "Debug.Exceptions", "Debug.BreakAll", "Debug.Breakpoints", "Debug.AddParallelWatch", "Debug.Watch1", "Debug.Modules", 
                "Debug.Output", "Debug.Print", "Debug.DeleteAllBreakpoints", "TestExplorer.DebugSelectedTests", 
                "TestExplorer.DebugAllTestsInContext", "TestExplorer.DebugAllTests", "View.Locals"
				));
		
		ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
		
		for(int i = 0; i < logFile.messages.size(); i++)
		{
			//just populate the array list with dummy values
			sequenceIDs.add(0);
		}
		
		
		Date lastTimestamp = logFile.timestamps.get(0);
		
		
		//TODO: change the index later to an input value which changes based on the index of parallel map phase
		//i.e: index*10.000.000
		int index = startIndex;
		
		for(int i = 0; i < logFile.messages.size(); i++)
		{
			Date curTimestamp = logFile.timestamps.get(i);
			String curMessage = logFile.messages.get(i);
			
			long timeDiff = curTimestamp.getTime() - lastTimestamp.getTime();
			
			long timeDiffSeconds = TimeUnit.MILLISECONDS.toSeconds(timeDiff);			
			//if we have a debug message:
			
			if(messagesDebug.contains(curMessage))
			{
				sequenceIDs.set(i, index);
				
				//if more than 30 seconds have elapsed
				if(timeDiffSeconds >= 30)
				{
					index = index + 1;					
				}
				lastTimestamp = curTimestamp;
			}
			else
			{
				if(timeDiffSeconds <= 30)
				{
					sequenceIDs.set(i, index);
				}
				else
				{
					sequenceIDs.set(i, 0);
				}
			}			
		}
		
		ArrayList<Date> timestamps = new ArrayList<Date>();
		ArrayList<Integer> devIDs = new ArrayList<Integer>();
		ArrayList<String> messages = new ArrayList<String>();
		ArrayList<Integer> sequenceIDsFinal = new ArrayList<Integer>();
		
		//now cycle through all the lines and eliminate those lines that have a sequence ID of 0 (they don't belong to any sequence). 
		for(int i = 0; i < sequenceIDs.size(); i++)
		{
			//firstly, identify whether the current message is an outlier (i.e: in the messagesRemove)
			//and in that case removes the corresponding timestamp and dev ID as well. 
			int seqID = sequenceIDs.get(i);
			
			if(seqID != 0)
			{
				devIDs.add(logFile.devIDs.get(i));
				messages.add(logFile.messages.get(i));
				timestamps.add(logFile.timestamps.get(i));
				sequenceIDsFinal.add(seqID);				
			}
		}
		
		logFile.setDevIDs(devIDs);
		logFile.setTimeStamps(timestamps);
		logFile.setMessages(messages);
		logFile.setSequenceID(sequenceIDsFinal);
		
		int amountSequencesIdentified = sequenceIDsFinal.get(sequenceIDsFinal.size() - 1) - startIndex + 1;
		
		System.out.println("Amount of sequences identified: " + amountSequencesIdentified);
		
		return logFile;
	}
	
	//Input: a logFile with the sequenceIDs marked
	//Output: a logFile where the sequences that have too small(< 2 minutes) or too long (> 100 minutes)
	//have been filtered out
	private static LogFile removeLongSmallSequences(LogFile logFileSeqID) 
	{
		
		//list of all those sequenceIDs that have been found to be valid (i.e: respect the constraints specified
		ArrayList<Integer> validSeqIDs = new ArrayList<Integer>();
		
		//this arrayList contains the unique occurrences of sequenceIDs. used to quickly iterate through all sequenceIDs
		ArrayList<Integer> uniqueSeqIDs = (ArrayList<Integer>) logFileSeqID.sequenceIDs.stream().distinct().collect(Collectors.toList());
		
		for(int i = 0; i < uniqueSeqIDs.size(); i++)
		{
			int curSeqID = uniqueSeqIDs.get(i);
			//now need to find: first occurrence and last occurrence of a seqID in the logFileSeqID.sequenceIDs
			int indexStartSequence = logFileSeqID.sequenceIDs.indexOf(curSeqID);
			int indexEndSequence = logFileSeqID.sequenceIDs.lastIndexOf(curSeqID);
			
			//now get the corresponding timestamps and get the time difference. 
			Date timestampStart = logFileSeqID.timestamps.get(indexStartSequence);
			Date timestampEnd = logFileSeqID.timestamps.get(indexEndSequence);
			
			//get the time difference in seconds
			long timeDiff = timestampEnd.getTime() - timestampStart.getTime();
			
			long timeDiffSeconds = TimeUnit.MILLISECONDS.toSeconds(timeDiff);	
			
			if(timeDiffSeconds >= 120 && timeDiffSeconds <= 6000)
			{
				validSeqIDs.add(curSeqID);
			}
		}
		
		//fine, all proper sequenceIDs generated. now just get all those lines that satisfy the sequenceIDs that have been found to be valid. 
		ArrayList<Date> timestamps = new ArrayList<Date>();
		ArrayList<Integer> devIDs = new ArrayList<Integer>();
		ArrayList<String> messages = new ArrayList<String>();
		ArrayList<Integer> sequenceIDsFinal = new ArrayList<Integer>();
		
		for(int i = 0; i < logFileSeqID.sequenceIDs.size(); i++)
		{
			int seqID = logFileSeqID.sequenceIDs.get(i);
			
			//if valid sequenceID, then consider the whole line. Otherwise, discard it
			if(validSeqIDs.contains(seqID))
			{
				devIDs.add(logFileSeqID.devIDs.get(i));
				messages.add(logFileSeqID.messages.get(i));
				timestamps.add(logFileSeqID.timestamps.get(i));
				sequenceIDsFinal.add(seqID);				
			}
		}
		
		logFileSeqID.setDevIDs(devIDs);
		logFileSeqID.setTimeStamps(timestamps);
		logFileSeqID.setMessages(messages);
		logFileSeqID.setSequenceID(sequenceIDsFinal);
		
		return logFileSeqID;
	}
	
	//Input: an arraylist of LogFile objects
	//Output: a LogFile in which all the input LogFile objects have been merged. 
	public static LogFile reduceArrayLists(ArrayList<LogFile> logFiles)
	{
		ArrayList<Date> timestamps = new ArrayList<Date>();
		ArrayList<Integer> devIDs = new ArrayList<Integer>();
		ArrayList<String> messages = new ArrayList<String>();
		ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
		
		for(int i = 0; i < logFiles.size(); i++)
		{
			//vectorial ops, way more efficient than cyling through each element
			timestamps.addAll(logFiles.get(i).timestamps);
			devIDs.addAll(logFiles.get(i).devIDs);
			messages.addAll(logFiles.get(i).messages);
			sequenceIDs.addAll(logFiles.get(i).sequenceIDs);
		}
		LogFile logFileReturn = new LogFile(timestamps, devIDs, messages, sequenceIDs);
		return logFileReturn; 
				
	}

}
