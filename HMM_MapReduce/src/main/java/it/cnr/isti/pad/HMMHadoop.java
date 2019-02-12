//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

package it.cnr.isti.pad;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HMMHadoop
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
			List<Date> timestampsSeq = new ArrayList<Date>();
			List<Integer> devIDsSeq = new ArrayList<Integer>();
			List<String> messagesSeq = new ArrayList<String>();
			List<Integer> sequenceIDsSeq = new ArrayList<Integer>();
			
			Sequence sequence = null;
			
	        IntWritable sequenceID = new IntWritable();
	        
			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
						
			System.out.println("Loaded string content from " + filePath);
			byte[] bytesRead = bytesContent.getBytes();
			//System.out.println("Loaded bytes into byte array ");			
		    
		    //ArrayList<Character> charArrayList = new ArrayList<Character>();
		    
		    //used to keep track of a single line. 
		    int j = 0; 
		    
		    StringBuilder line = new StringBuilder();

			char charCur;
			
			int startIndexFile = Utilities.extraSeqIDFromFilePath(filePath);
		        
	        int index = startIndexFile * seqIDMultiplier;
	        
	        //used to keep track of how many valid messages are present
	        int messageIndex = 0;
	        
			Date lastTimestamp = null;
			Date curTimestamp = null;
			
			String[] allColumns;
			String curMessage;
			
			int messagesInSequences = 0;
			int devID;
			
			long timeDiff = 0;
			long timeDiffSeconds = 0;
			
			//process the input text file character-by-character
			for(int i = 0; i < bytesRead.length; i++)
			{
				charCur = (char) bytesRead[i];
				
				//reached the end of a line, meaning we are able to process the line.
			    if(charCur == '\n')
				{
					//avoid the first line, which is the header.
					if(j != 0)
					{
						allColumns = line.toString().split(separator);
				        
				        //firstly, parse the timestamp, devID, message
						try {
							curTimestamp = dateFormat.parse(allColumns[0]);
						} catch (ParseException e) 
						{
							break;
						}    
						devID = new Integer(allColumns[1]);   
						curMessage = new String(allColumns[2]);
						
				        //if the current message is a messageToRemove, then just skip the current line (i.e: outlier symbols are removed)
				        if(!messagesRemove.contains(curMessage))
				        {
				        	//first message being parsed, then set the lastTimestamp as well.
				        	if(messageIndex == 0)
				        	{
				        		lastTimestamp = curTimestamp;
				        	}
				        			        	
							timeDiff = curTimestamp.getTime() - lastTimestamp.getTime();
							timeDiffSeconds = TimeUnit.MILLISECONDS.toSeconds(timeDiff);	
							
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
								        
								        messagesInSequences += messagesSeq.size();
								        
									}
									//empty the current arraylists
							        timestampsSeq.clear();
						        	devIDsSeq.clear();
						        	messagesSeq.clear();
						        	sequenceIDsSeq.clear();
						        	
							        //anyway, need to create a new sequence with a brand new index
									index = index + 1;	
								}
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
								}
							}			
					        messageIndex++;
				        }
						
					}
					line = new StringBuilder();
					j++;
				}
			    //add every single character to the current line
			    else
			    {
			    	line.append(charCur);
			    }
			}
			
			 if(messagesSeq.size() > 0 && timeDiffSeconds >= 120 && timeDiffSeconds <= 6000)
		     {
	        	 sequence = new Sequence(timestampsSeq, devIDsSeq, messagesSeq, sequenceIDsSeq, filePath);
	        	 sequenceID.set(index);
	        	 context.write(sequenceID, sequence);
			     messagesInSequences += messagesSeq.size();
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
		
			System.out.println("Successfully read all bytes as characters");
		}
	}
	

	//Input: - IntWritable key: the unique sequenceID of the sequence being considered
	//		 - LogFile value: the input logFile received
	//Output: - IntWritable key: the sequenceID of the sequence to output 
	//		  - value: Sequence corresponding to this sequenceID
	public static class NewReducer extends Reducer<IntWritable, Sequence, IntWritable, Sequence>
	{
		//used to store all the different messages in all sequences read. 
		List<String> messagesStored = new ArrayList<String>();
		List<Sequence> sequencesStored = new ArrayList<Sequence>();
		
		//size of the largest sequence
		int maxSizeSequence = 0;		
		
		//Output the total amount of sequences identified. 
		public void reduce(IntWritable key, Iterable<Sequence> sequences, Context context) throws IOException, InterruptedException
		{		
			//System.out.println(key);
			//issue: we read the current sequence along with all the preceding values in readFields. 
			for(Sequence sequence : sequences)
			{
				//System.out.println(sequence);
				messagesStored.addAll(sequence.messages);
				sequencesStored.add(sequence);
				
				if(sequence.messages.size() > maxSizeSequence)
				{
					maxSizeSequence = sequence.messages.size();
				}
				context.write(key, sequence);	
			}
		}
		
		
		
	   @Override
       protected void cleanup(Context context) throws IOException, InterruptedException 
	   {		   
		   //identify all the unique messages within the messages identified (i.e: symbols).
		   HashSet<String> symbolsDistinct = new HashSet<>();		   
		   //Used to solve the 'null values' in observations problem (i.e: sequences must have a
		   //fixed length, so we add an empty string up to the maxString length		   
		   symbolsDistinct.addAll(messagesStored);
		   
		   String[] symbols = symbolsDistinct.toArray(new String[symbolsDistinct.size()]);
		   
		   //all the messages in all sequences
		   //String[] observationsStr = messagesStored.toArray(new String[messagesStored.size()]);
		   //need to convert the observations into an array of integers
		   //int[] observationsInt = Utilities.convertObsIntoIntArray(symbols, observationsStr);
		   
		   String[][] observationsMatrix = Utilities.populateObservationsMatrix(maxSizeSequence, sequencesStored);
		   
		   HMM<String> hmmTrained = Utilities.createTrainHMM(symbols, observationsMatrix);
		   
			System.out.println("Trained HMM" + hmmTrained);

		   System.out.println("Amount of sequences to process " + sequencesStored.size());
		   System.out.println("Amount of messages to process " + messagesStored.size());
		   System.out.println("Maximum amount of messages in a sequence " + maxSizeSequence);
		   
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
		
		job.setJarByClass(HMMHadoop.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		//need to process whole file, not just single line here.
		//Mapper output values
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Sequence.class);
		
		//need to create the HMM based on all the sequences 
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