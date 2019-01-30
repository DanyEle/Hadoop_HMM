import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Main {

	public static void main(String[] args) throws FileNotFoundException, ParseException, IOException {
		
		String filePath1 = "/home/hadoop/ABB_Logs/BlazeData_Dev_27.csv";
		
		//messagesFilter represent outlier symbols to remove. 
		ArrayList<String> messagesRemove = new ArrayList<>(Arrays.asList("View.OnChangeCaretLine", "View.OnChangeScrollInfo", "View.File", 
				"Debug.Debug Break Mode", "Debug.Debug Run Mode","Debug.DebugType", "Debug.Enter Design Mode","Build.BuildDone", "Build.BuildBegin"));

		String fileContent1 = loadFilterSingleDataset(filePath1, messagesRemove);


	}
	
	//Input: the path of a file to be loaded and filtered. 
	//Output: the file with three columns and a sequenceID assigned to contiguous sequences.
	public static String loadFilterSingleDataset(String filePath, ArrayList<String> messagesRemove) throws FileNotFoundException, ParseException, IOException
	{
		
		//we have successfully loaded the file from the disk. 
		LogFile logFile = readArraysFromFile(filePath);
		
		LogFile logFileNoOutliers = removeOutliers(logFile, messagesRemove);
		
		
		
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
		//now iterate through all the logs' lines and remove all those lines 
		//that contain a message which is found to be an outlier. 
		for(int i = 0; i < logFile.messages.size(); i++)
		{
			//firstly, identify whether the current message is an outlier (i.e: in the messagesRemove)
			//and in that case removes the corresponding timestamp and dev ID as well. 
			String message = logFile.messages.get(i);
			
			if(messagesRemove.contains(message))
			{
				logFile.messages.remove(i);
				logFile.timestamps.remove(i);
				logFile.devIDs.remove(i);
			}
		}
		return logFile;		
	}

}
