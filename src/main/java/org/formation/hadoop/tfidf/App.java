package org.formation.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Hello World!");
		Path inputFilePath = new Path("tfidf/input");
		Path outputFilePath = new Path("tfidf/output");

		// Job configuration
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TfIdfWordCount");
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		// set ouput key as Text and value as IntWritable
		job.setOutputKeyClass(WordDocnameWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// add Map as the Mapper used by the job
		job.setMapperClass(MapperTfidfWordCount.class);
		job.setReducerClass(ReducerTfidfWordCount.class);
		
		// Add stopwords to cachedFiles
		job.addCacheFile(new Path("tfidf/stopwords/stopwords_en.txt").toUri());

		job.setJarByClass(App.class);

		job.waitForCompletion(true);

	}
}
