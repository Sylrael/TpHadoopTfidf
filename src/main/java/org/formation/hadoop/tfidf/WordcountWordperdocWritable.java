package org.formation.hadoop.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class WordcountWordperdocWritable implements Writable {
		private IntWritable wordCount = new IntWritable();
		private IntWritable wordPerDoc = new IntWritable();

	
	public void readFields(DataInput arg0) throws IOException {
		wordCount.readFields(arg0);
		wordPerDoc.readFields(arg0);		
	}

	public void write(DataOutput arg0) throws IOException {
		wordCount.write(arg0);
		wordPerDoc.write(arg0);
	}
	
	public IntWritable getWordCount() {
		return wordCount;
	}

	public void setWordCount(IntWritable wordCount) {
		this.wordCount = wordCount;
	}

	public IntWritable getWordPerDoc() {
		return wordPerDoc;
	}

	public void setWordPerDoc(IntWritable wordPerDoc) {
		this.wordPerDoc = wordPerDoc;
	}

	@Override
	public String toString() {
		return wordCount+"\t"+wordPerDoc;
	}	
	
}
