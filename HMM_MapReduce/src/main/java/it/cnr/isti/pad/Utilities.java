//PAD - Distributed Enabling Platforms
//University of Pisa 2019
//Author: Daniele Gadler

package it.cnr.isti.pad;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class Utilities 
{
	
	public Utilities()
	{
		
	}
	
	public static String[][] populateObservationsMatrix(int maxSizeSequence, List<Sequence> sequencesStored)
	{
		 String[][] observationsMatrix = new String[sequencesStored.size()][maxSizeSequence];

		 for(int i = 0; i < sequencesStored.size(); i++)
		   {
			   //j --> loop over all messages in a sequence
			   for(int j = 0; j < sequencesStored.get(i).messages.size(); j++)
			   {
				   //get the j-th message of the i-th sequence. 
				   observationsMatrix[i][j] = sequencesStored.get(i).messages.get(j);
			   }
			   //all those not part of a 
			   for(int j = sequencesStored.get(i).messages.size(); j < maxSizeSequence; j++)
			   {
				   observationsMatrix[i][j] = null;
			   }
		   }
		 
		 return  observationsMatrix;
		
	}

	public static int extraSeqIDFromFilePath(String filePath)
	{
		 String regex = "[^\\/]+$";
		 Pattern pattern = Pattern.compile(regex);
		 Matcher matcher = pattern.matcher(filePath);
		 
		 //firstly, get the fileName
		 if (matcher.find()) 
		 {
			  String fileName = matcher.group(0);
			  //now use another regex to get the fileNumber within the fileName
			  regex = "[0-9]+";
			  pattern = Pattern.compile(regex);
			  
			  matcher = pattern.matcher(fileName);
			  
			  //i.e: the filename actually does contain a number
			  if(matcher.find())
			  {
				  int fileNumber = new Integer(matcher.group(0));
				  return fileNumber;
			  }
			  //the filename does not contain a number
			  else
			  {
				 try 
				 {
					throw new ParseException("The filename at [" + filePath + "] does not contain a number!", 0);
				 } 
				 catch (ParseException e) 
				 {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return -1;
			 }		
		 }
		 
		 return 0;
		
	}
	
	
	public static String readFile(String path, Charset encoding) throws IOException 
	{
	  byte[] encoded = Files.readAllBytes(Paths.get(path));
	  return new String(encoded, encoding);
	}
	
	public static String loadFileContent(String filePath) throws IOException
	{
		String content = readFile(filePath, StandardCharsets.UTF_8);
		return content;
	}
	
	
	
	//Input: String[] symbols: an array containing all symbols in the dataset
	//		 String[] observations: an array containing all the observations in the dataset as strings
	//Output: int[]: an array containing all observations as integer array based on a mapping of 1 symbols --> 1 integer
	public static int[] convertObsIntoIntArray(String[] symbols, String[] observationsStr)
	{
		int[] observationsInt = new int[observationsStr.length];
		
		HashMap<String, Integer> symbolsMapping = new HashMap<String, Integer>();
		for(int i = 0; i < symbols.length; i++)
		{
			symbolsMapping.put(symbols[i], i);
		}
		
		//now check for every occurrence of every observation which integer corresponds to it and 
		//add it to the array of integers
		
		
		for(int i = 0; i < observationsStr.length; i++)
		{
			observationsInt[i] = symbolsMapping.get(observationsStr[i]);
		}
		
		return observationsInt;
	}
	
	
	public static HMM<String> createTrainHMM(String[] symbols, String[][] observationsMatrix)
	{
		System.out.println("HMM Initialized");
		
		//initial-state probabilities
		double[] pi = new double[1];
		pi[0]= 1;
		
		//set the transition probabilities
		double[][] transProbs = new double[1][1];
		transProbs[0][0] = 1;
		
		//set the emission probabilities
		double[][] emissProbs = initializeEmissProbs(symbols);
		
		HMM<String> hmmInit = new HMM<String>(pi, transProbs, emissProbs, symbols);
		System.out.println("Initialized HMM" + hmmInit.toString());

		
		HMM<String> hmmTrained = hmmInit.learn(observationsMatrix, 100);
		
		return hmmTrained;

	}
	
	
	//Input: String[] symbols: a list of symbols
	public static double[][] initializeEmissProbs(String[] symbols)
	{
		//empty matrix for emission probabilities
		double[][] emissProbInit = new double[1][symbols.length];
		
		double[] randProbabilities = randSum(symbols.length, 1);

		//now populate the emission probabilities matrix with the probabilities generated
		for(int i = 0; i < randProbabilities.length; i++)
		{
			emissProbInit[0][i] = randProbabilities[i];
		}
		
		return emissProbInit;
	}
	
	//generate n numbers that sum up to n. 
	private static double[] randSum(int n, double m) {
	    Random rand = new Random();
	    double randNums[] = new double[n], sum = 0;

	    for (int i = 0; i < randNums.length; i++) {
	        randNums[i] = rand.nextDouble();
	        sum += randNums[i];
	    }

	    for (int i = 0; i < randNums.length; i++) {
	        randNums[i] /= sum * m;
	    }

	    return randNums;
	}
	
	

}
