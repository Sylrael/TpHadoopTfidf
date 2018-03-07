package org.formation.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTfidfWordCount extends Reducer<WordDocnameWritable, IntWritable, WordDocnameWritable, IntWritable>{
	
	protected void reduce(WordDocnameWritable cle, Iterable<IntWritable> valeurs, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value : valeurs) {
			count++;
		}
		context.write(cle, new IntWritable(count));
	}
}
