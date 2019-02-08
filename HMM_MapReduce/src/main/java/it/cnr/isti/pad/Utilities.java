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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utilities 
{
	
	public Utilities()
	{
		
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
	

}
