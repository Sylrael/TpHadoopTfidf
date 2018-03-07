package org.formation.hadoop.tfidf.job2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.formation.hadoop.tfidf.WordDocnameWritable;

public class MapperTfidfWordPerDoc extends Mapper<LongWritable, Text, WordDocnameWritable, IntWritable> {
	IntWritable valeur = new IntWritable(1);
	WordDocnameWritable cle = new WordDocnameWritable();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");

		// Treating file
		String filename = tokens.nextToken();
		String mot = tokens.nextToken();
		int nbOccurrence = Integer.parseInt(tokens.nextToken());
		valeur.set(nbOccurrence);
		cle.setDocId(new Text(filename));
		cle.setMot(new Text(mot));
		context.write(cle, valeur);
	}
}