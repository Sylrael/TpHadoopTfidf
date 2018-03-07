package org.formation.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.formation.hadoop.tfidf.job2.GroupingComparatorWordPerDoc;
import org.formation.hadoop.tfidf.job2.MapperTfidfWordPerDoc;
import org.formation.hadoop.tfidf.job2.PartitionerWordPerDoc;
import org.formation.hadoop.tfidf.job2.ReducerTfidfWordPerDoc;
import org.formation.hadoop.tfidf.job3.GroupingComparatorCalculTfidf;
import org.formation.hadoop.tfidf.job3.MapperTfidfCalculTfidf;
import org.formation.hadoop.tfidf.job3.PartitionerCalculTfidf;
import org.formation.hadoop.tfidf.job3.ReducerTfidfCalculTfidf;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Hello World!");
		Path inputFilePath = new Path("tfidf/input");
		Path outputFilePath = new Path("tfidf/output/job1");

		Path inputFilePath2 = new Path("tfidf/output/job1/part-r-00000");
		Path outputFilePath2 = new Path("tfidf/output/job2");
		
		Path inputFilePath3 = new Path("tfidf/output/job2/part-r-00000");
		Path outputFilePath3 = new Path("tfidf/output/job3");

		// Job 1
		//launchJobWordCount(inputFilePath, outputFilePath);
		// Job 2
		//launchJobWordPerDoc(inputFilePath2, outputFilePath2);
		// Job 3
		launchJob3(inputFilePath3, outputFilePath3);

	}

	public static void launchJobWordCount(Path in, Path out)
			throws IOException, ClassNotFoundException, InterruptedException {
		// Job configuration
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TfIdfWordCount");
		
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		// Job 1 : Calcul du nombre d'occurences des mots dans des documents
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

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

	public static void launchJobWordPerDoc(Path in, Path out)
			throws IOException, ClassNotFoundException, InterruptedException {
		// Job configuration

		Configuration conf = new Configuration();
		Job job = new Job(conf, "TfIdfWordPerDoc");
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		// Job 2 : Calcul de WordPerDoc : nombre total de mots dans chaque document
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// set ouput key as Text and value as IntWritable
		job.setMapOutputKeyClass(WordDocnameWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WordDocnameWritable.class);
		job.setOutputValueClass(WordcountWordperdocWritable.class);

		// Partitioning/Grouping
		job.setPartitionerClass(PartitionerWordPerDoc.class);
		job.setGroupingComparatorClass(GroupingComparatorWordPerDoc.class);

		// add Map as the Mapper used by the job
		job.setMapperClass(MapperTfidfWordPerDoc.class);
		job.setReducerClass(ReducerTfidfWordPerDoc.class);

		job.setJarByClass(App.class);
		job.waitForCompletion(true);
	}

	public static void launchJob3(Path in, Path out) throws IOException, ClassNotFoundException, InterruptedException {
		// Job configuration

		Configuration conf = new Configuration();
		Job job = new Job(conf, "TfIdfCalculTfIdf");
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		// Job 3 : Calcul du Tf-Idf
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// set ouput key as Text and value as IntWritable
		job.setMapOutputKeyClass(WordDocnameWritable.class);
		job.setMapOutputValueClass(WordcountWordperdocWritable.class);
		job.setOutputKeyClass(WordDocnameWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Partitioning/Grouping
		job.setPartitionerClass(PartitionerCalculTfidf.class);
		job.setGroupingComparatorClass(GroupingComparatorCalculTfidf.class);

		// add Map as the Mapper used by the job
		job.setMapperClass(MapperTfidfCalculTfidf.class);
		job.setReducerClass(ReducerTfidfCalculTfidf.class);

		job.setJarByClass(App.class);
		job.waitForCompletion(true);
	}

}
