package org.formation.hadoop.tfidf.job3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.formation.hadoop.tfidf.WordDocnameWritable;
import org.formation.hadoop.tfidf.WordcountWordperdocWritable;

public class MapperTfidfCalculTfidf extends Mapper<LongWritable, Text, WordDocnameWritable, WordcountWordperdocWritable> {
	WordDocnameWritable cle = new WordDocnameWritable();
	WordcountWordperdocWritable valeur = new WordcountWordperdocWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");

		// Treating file
		String filename = tokens.nextToken();
		String mot = tokens.nextToken();
		int nbOccurrence = Integer.parseInt(tokens.nextToken());
		int nbWordPerDoc = Integer.parseInt(tokens.nextToken());
		valeur.setWordCount(new IntWritable(nbOccurrence));
		valeur.setWordPerDoc(new IntWritable(nbWordPerDoc));
		cle.setDocId(new Text(filename));
		cle.setMot(new Text(mot));
		context.write(cle, valeur);
	}
}