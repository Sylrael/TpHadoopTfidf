	package org.formation.hadoop.tfidf;
	import java.io.IOException;

	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mrunit.mapreduce.MapDriver;
	import org.junit.Before;
	import org.junit.Test;

	public class WordCountMRUnitTest {
		MapDriver <LongWritable, Text, WordDocnameWritable, IntWritable> mapDriver;
		
		@Before
		public void setUp(){
			MapperTfidfWordCount mapperToTest = new MapperTfidfWordCount();
			mapDriver = MapDriver.newMapDriver(mapperToTest);
		}
		
		@Test
		public void testMapper() throws IOException {
			mapDriver.withInput(new LongWritable(), new Text("test word count test"));
			mapDriver.withInput(new LongWritable(), new Text("test hadoop count"));
			mapDriver.withOutput(new WordDocnameWritable(new Text("test"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("word"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("count"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("test"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("test"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("hadoop"), new Text("somefile")), new IntWritable(1));
			mapDriver.withOutput(new WordDocnameWritable(new Text("count"), new Text("somefile")), new IntWritable(1));
			mapDriver.runTest();
		}
	}
