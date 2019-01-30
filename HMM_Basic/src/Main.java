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

public class Main {

	public static void main(String[] args) throws FileNotFoundException, ParseException, IOException {
		
		String filePath1 = "/home/hadoop/ABB_Logs_Small/BlazeData_Dev_33.csv";
		
		System.out.println("Loading data from " + filePath1);
		
		
		//messagesFilter represent outlier symbols to remove. 
		ArrayList<String> messagesRemove = new ArrayList<>(Arrays.asList("View.OnChangeCaretLine", "View.OnChangeScrollInfo", "View.File", 
				"Debug.Debug Break Mode", "Debug.Debug Run Mode","Debug.DebugType", "Debug.Enter Design Mode","Build.BuildDone", "Build.BuildBegin"));

		String fileContent1 = loadFilterSingleDataset(filePath1, messagesRemove);


	}
	
	//Input: the path of a file to be loaded and filtered. 
	//Output: the file with three columns and a sequenceID assigned to contiguous sequences.
	public static String loadFilterSingleDataset(String filePath, ArrayList<String> messagesRemove) throws FileNotFoundException, ParseException, IOException
	{
		
		//we have successfully loaded the file from the disk into three array lists
		LogFile logFile = readArraysFromFile(filePath);
		System.out.println("Amount of messages before removing outliers: " + logFile.messages.size());
		
		//we have successfully remove all those lines that contain an outlier message.
		LogFile logFileNoOutliers = removeOutliers(logFile, messagesRemove);
		
		System.out.println("Amount of messages after removing outliers: " + logFileNoOutliers.messages.size());
		
		//we have successfully marked every single message with a sequence ID
		LogFile logFileSeqID = markSequenceIDs(logFileNoOutliers);
		
		
		
		//NB: need to change the return value, eventually. 
		return "";
	}
	
	//Input: the path of a file containing three columns: timestamp, dev_id, message
	//Output: an object storing each one of the three different columns in array lists for easy access. 
	public static LogFile readArraysFromFile(String filePath) throws ParseException, FileNotFoundException, IOException
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
                //add the timestamp, the DevID and the message to the respective array lists
                timestamps.add(parsedTimestamp);
                devIDs.add(devId);
                messages.add(message);                
                         
               // System.out.println("Timestamp = " + allColumns[0] + " , dev_id=" + allColumns[1] + ", message = " + allColumns[2]);
            }
        } 
        
        //all data loaded into the respective array lists, fine. Now put it into a class
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
	
	public static LogFile markSequenceIDs(LogFile logFile)
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
		int index = 1;
		
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
		
		//now cycle through all the lines and eliminate those that have a sequence ID of 0 (they don't belong to any sequence). 
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
		
		System.out.println("Amount of sequences identified: " + sequenceIDsFinal.get(sequenceIDsFinal.size() - 1));
		
		return logFile;
	}

}
