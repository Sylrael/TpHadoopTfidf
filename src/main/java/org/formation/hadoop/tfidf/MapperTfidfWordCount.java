package org.formation.hadoop.tfidf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperTfidfWordCount extends Mapper<LongWritable, Text, WordDocnameWritable, IntWritable>{
	IntWritable valeur = new IntWritable(1);
	WordDocnameWritable cle = new WordDocnameWritable();
	
	String filename = "";
	Path[] cachedFiles;
	Set<String> stopwordsList = new HashSet<String>();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\" ,:();-.?!*+%/\\<>|_][{}0123456789\t");
		// Retrieving cached file containing stopwords to skip
		cachedFiles = context.getLocalCacheFiles();
		
		// Retrieving name of file to treat
		filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		// Creating Set of stopwords
		BufferedReader fis = new BufferedReader (new FileReader(cachedFiles[0].toString()));
		String stopword = "";
		while ((stopword = fis.readLine()) != null) {
			stopwordsList.add(stopword);
		}
		
		// Treating file
		while (tokens.hasMoreTokens())
		{
			String mot = tokens.nextToken().toLowerCase();
			if (mot.length() > 2 && !stopwordsList.contains(mot)) {
				cle.setMot(new Text(mot));
				cle.setDocId(new Text(filename));
				context.write(cle, valeur);
			}
		}
	}
}