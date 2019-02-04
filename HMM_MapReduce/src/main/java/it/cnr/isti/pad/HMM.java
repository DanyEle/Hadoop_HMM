package it.cnr.isti.pad;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HMM
{
	public static class NewMapper extends Mapper<NullWritable, BytesWritable, Text, LogFile>
	{
		//set of outlier objs to be removed
		private final static ArrayList<String> messagesRemove = new ArrayList<>(Arrays.asList("View.OnChangeCaretLine", "View.OnChangeScrollInfo", "View.File", 
				"Debug.Debug Break Mode", "Debug.Debug Run Mode","Debug.DebugType", "Debug.Enter Design Mode","Build.BuildDone", "Build.BuildBegin"));
		
		private final static int seqIDMultiplier = 10000000;
		
		//we're going to need to output the "LogFile" object to the reducer --> will need for a way to serialize it.
		LogFile logFile;
		
		//used as a starting point to assign a sequenceID to every single message.  
		int seqIDStart;
		
		Text keyFileWrite = new Text();
		
		//Input: - NullWritable key: a key that is NOT used for indexing a whole file
		//		 - BytesWritable bytesContent: the content of the file being parsed in byte format.
		//Output:- Text key: the (unique) path of the file being parsed
		//		 - LogFile value: the logFile being passed over to the reducer 
		
		//every single map function gets a separate log file as input.
		public void map(NullWritable key, BytesWritable bytesContent, Context context) throws IOException, InterruptedException
		{			
			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			String fileContent = new String(bytesContent.getBytes());
			
			System.out.println("Loading data from " + filePath);
			
			//firstly, populate the logFile with the content of the textFile being loaded
			logFile = Utilities.readArraysFromFileContent(fileContent, filePath);
			System.out.println("Amount of messages before removing outliers: " + logFile.messages.size());
			//remove all those lines that contain an outlier message.
			logFile = Utilities.removeOutliers(logFile, messagesRemove);
			System.out.println("Amount of messages after removing outliers: " + logFile.messages.size());			//now group messages based on their sequence IDs.
			seqIDStart = Utilities.extraSeqIDFromFilePath(filePath) * seqIDMultiplier;
			//EX: Sequence ID 31 --> 31.000.000. This is done to avoid having 
			//sequenceIDs from different files overlapping
			logFile = Utilities.markSequenceIDs(logFile, seqIDStart);
			System.out.println("Amount of messages after marking sequence IDs and removing messages not in sequences): " + logFile.sequenceIDs.size());
			//now remove all the sequences that are too small (< 2 minutes in length) and large sessions (> 100 minutes in length)
			logFile = Utilities.removeLongSmallSequences(logFile);
			System.out.println("Amount of messages after after removing long and small sequences: " + logFile.sequenceIDs.size());
			//we consider the filename's path as the unique key of the logFile. 
			keyFileWrite.set(filePath);
			
			context.write(keyFileWrite, logFile);
		}
		
		
		
	}
	

	//Input: - Text key: the (unique) path of the file being parsed
	//		 - LogFile value: the input logFile received
	//Output: - Text key: the filename 
	//		  - value: logFile corresponding to this key
	//dummy reducer for now.
	public static class NewReducer extends Reducer<Text, LogFile, Text, LogFile>
	{
		//Output the total amount of sequences identified. 
		public void reduce(Text key, Iterable<LogFile> logFiles, Context context) throws IOException, InterruptedException
		{
			System.out.println("Key received" + key);
			
			for(LogFile logFile : logFiles)
			{
				context.write(key, logFile);
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
	        System.err.println("Failed to delete temporary 'output' path");
	    }
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "hmm");
		
		job.setJarByClass(HMM.class);
		
		//need to process whole file, not just single line here.
		//Mapper output values
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LogFile.class);
		job.setInputFormatClass(WholeFileInputFormat.class);

		//Reduce output values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//just for testing purposes, try merging with one single reducer
		//job.setNumReduceTasks(1);


		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
