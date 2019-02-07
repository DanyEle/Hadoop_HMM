package it.cnr.isti.pad;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HMM
{
	public static class NewMapper extends Mapper<NullWritable, BytesWritable, IntWritable, Sequence>
	{
		
		//STATIC READ-ONLY DATA
		private final static ArrayList<String> messagesRemove = new ArrayList<>(Arrays.asList("View.OnChangeCaretLine", "View.OnChangeScrollInfo", "View.File", 
		"Debug.Debug Break Mode", "Debug.Debug Run Mode","Debug.DebugType", "Debug.Enter Design Mode","Build.BuildDone", "Build.BuildBegin"));
		
		private final static int seqIDMultiplier = 100000;
		
		//these are the IDE debug messages, which act as a starting point for the interaction of a developer with the IDE
		private final static ArrayList<String> messagesDebug = new ArrayList<>(Arrays.asList(
				"Debug.ToggleBreakpoint", "Debug.CallStack", "View.Call Stack", "Debug.Start", "Debug.StepOver", "Debug.StepInto", "Debug.StepOut", 
                "Debug.AttachtoProcess", "Debug.StartDebugTarget", "Debug.StopDebugging", "Debug.QuickWatch", "Debug.AddWatch", "View.Watch 1", 
                "Debug.DisableAllBreakpoints", "Debug.DetachAll", "Debug.Restart", "Debug.RunToCursor", "Debug.EnableAllBreakpoints", 
                "Debug.ShowNextStatement", "Debug.BreakatFunction", "Debug.StartPerformanceAnalysis", "Debug.AddParallelWatch", 
                "Debug.Threads", "Debug.Disassembly", "Debug.GoToDisassembly", "Debug.EvaluateStatement", "Debug.SetNextStatement", 
                "Debug.Exceptions", "Debug.BreakAll", "Debug.Breakpoints", "Debug.AddParallelWatch", "Debug.Watch1", "Debug.Modules", 
                "Debug.Output", "Debug.Print", "Debug.DeleteAllBreakpoints", "TestExplorer.DebugSelectedTests", 
                "TestExplorer.DebugAllTestsInContext", "TestExplorer.DebugAllTests", "View.Locals"
				));
		
        private final static String separator = ",";
		
		
        private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        
    	
		
        
		
        //timestamps, devIDs, messages and sequenceIDs are grouped based on the sequenceID of that sequence

        
		//Input: - NullWritable key: a key that is NOT used for indexing a whole file
		//		 - BytesWritable bytesContent: the content of the file being parsed in byte format.
		//Output:- IntWritable key: the sequenceID of the current sequence being passed to the reducer
		//		 - Sequence value: the Sequence being passed over to the reducer 
		//Every single map function gets a separate log file as input.
		public void map(NullWritable key, BytesWritable bytesContent, Context context) throws IOException, InterruptedException
		{				
			ArrayList<Date> timestampsSeq = new ArrayList<Date>();
			ArrayList<Integer> devIDsSeq = new ArrayList<Integer>();
			ArrayList<String> messagesSeq = new ArrayList<String>();
			ArrayList<Integer> sequenceIDsSeq = new ArrayList<Integer>();
			
			Sequence sequence = null;
			
	        IntWritable sequenceID = new IntWritable();
	        
			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
						
			byte[] bytesRead = bytesContent.getBytes();
			System.out.println("Loading bytes content from " + filePath);
			
			//CharSequence charSequence = new Char
			//String fileContent = new String(bytesContent.getBytes());
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append(bytesRead);
			strBuilder.trimToSize();
	        
			//process the input text file line by line. 
	        StringTokenizer itr = new StringTokenizer(strBuilder.toString(), "\n");
	        
	        //skip the first line, which is the header.
	        itr.nextToken();
	        
	        //the current seqID is initialized based on the filename being loadded
	        int startIndexFile = Utilities.extraSeqIDFromFilePath(filePath);
	        
	        int index = startIndexFile * seqIDMultiplier;
	        
	        //used to keep track of how many valid messages are present
	        int messageIndex = 0;
	        int messagesInSequences = 0;
	        
			Date lastTimestamp = null;
			Date curTimestamp = null;
			
			String line;
			String[] allColumns;
			String curMessage;
			
			int devID;
			
	        while(itr.hasMoreTokens())
	        {	        	
	        	line = itr.nextToken();
		        allColumns = line.split(separator);
		        
		        //firstly, parse the timestamp, devID, message
				try {
					curTimestamp = dateFormat.parse(allColumns[0]);
				} catch (ParseException e) 
				{
					break;
				}    
				devID = new Integer(allColumns[1]);   
				curMessage = new String(allColumns[2]);
		        
		        //if the current message is a messageToRemove, then just skip the current line. 
		        if(!messagesRemove.contains(curMessage))
		        {
		        	//first message being parsed, then set the lastTimestamp as well.
		        	if(messageIndex == 0)
		        	{
		        		lastTimestamp = curTimestamp;
		        	}
		        			        	
					long timeDiff = curTimestamp.getTime() - lastTimestamp.getTime();
					long timeDiffSeconds = TimeUnit.MILLISECONDS.toSeconds(timeDiff);	
					
					if(messagesDebug.contains(curMessage))
					{				
						//add the current line to the current sequence.
						messagesSeq.add(curMessage);
						devIDsSeq.add(devID);
						timestampsSeq.add(curTimestamp);	
						sequenceIDsSeq.add(index);
						
						//if more than 30 seconds have elapsed, then need to create a new sequence. 
						if(timeDiffSeconds >= 30)
						{
							//if the time sequence is too small or too long, then we do not output it. 
							if(timeDiffSeconds >= 120 && timeDiffSeconds <= 6000)
							{
								//firstly save the current sequence.
						        sequence = new Sequence(timestampsSeq, devIDsSeq, messagesSeq, sequenceIDsSeq, filePath);
						        sequenceID.set(index);
						        context.write(sequenceID, sequence);
							}
							//empty the current arraylists
					        timestampsSeq.clear();
				        	devIDsSeq.clear();
				        	messagesSeq.clear();
				        	sequenceIDsSeq.clear();
				        	
					        //anyway, need to create a new sequence with a brand new index
							index = index + 1;	
						}
						messagesInSequences++;
						lastTimestamp = curTimestamp;
					}
					else
					{
						//within 30 seconds, then add the current devID, msg and timestamp to the current sequence.
						if(timeDiffSeconds <= 30)
						{
							messagesSeq.add(curMessage);
							devIDsSeq.add(devID);
							timestampsSeq.add(curTimestamp);
							sequenceIDsSeq.add(index);
							messagesInSequences++;
						}
					}			
			        messageIndex++;
		        }
	        } 
	        //if the end of the file has been reached and some messages have yet to be saved, then save them now.
	        if(messagesSeq.size() > 0)
	        {
	        	 sequence = new Sequence(timestampsSeq, devIDsSeq, messagesSeq, sequenceIDsSeq, filePath);
	        	 sequenceID.set(index);
	        	 context.write(sequenceID, sequence);
	        	 //empty the current arraylists
			     timestampsSeq.clear();
		         devIDsSeq.clear();
		         messagesSeq.clear();
		         sequenceIDsSeq.clear();
		         //and finally increase the sequence index
			     index = index + 1;	

	        }
	        System.out.println("Amount of messages identified after removing outliers: " + messageIndex);
	        System.out.println("Amount of sequences identified after removing outliers:  " + (index - (startIndexFile * seqIDMultiplier)));
	        System.out.println("Amount of messages after marking sequence IDs and removing those not in sequences " + messagesInSequences);
		}
	}
	

	//Input: - IntWritable key: the unique sequenceID of the sequence being considered
	//		 - LogFile value: the input logFile received
	//Output: - IntWritable key: the sequenceID of the sequence to output 
	//		  - value: Sequence corresponding to this sequenceID
	public static class NewReducer extends Reducer<IntWritable, Sequence, IntWritable, Sequence>
	{
		//Output the total amount of sequences identified. 
		public void reduce(IntWritable key, Iterable<Sequence> sequences, Context context) throws IOException, InterruptedException
		{		
			//group all sequences into an arraylist, then use these sequences to train a Hidden Markov Model
			//ArrayList<Sequence> sequencesStored = new ArrayList<Sequence>();
			
			//issue: we read the current sequence along with all the preceding values in readFields. 
			for(Sequence sequence : sequences)
			{
				context.write(key, sequence);
				//sequencesStored.add(sequence);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		//delete the "output" directory when starting up
		
		try 
		{
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path("./output"), true);
	    } 
		catch (IOException e) 
		{
	        System.err.println("Failed to 'output' folder.");
	    }
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "hmm");
		
		job.setJarByClass(HMM.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		//need to process whole file, not just single line here.
		//Mapper output values
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Sequence.class);
		
		job.setNumReduceTasks(1);
		

		//Reduce output values
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Sequence.class);
		
		//just for testing purposes, try merging with one single reducer

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
