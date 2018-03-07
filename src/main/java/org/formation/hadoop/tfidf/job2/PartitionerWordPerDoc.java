package org.formation.hadoop.tfidf.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.formation.hadoop.tfidf.WordDocnameWritable;

public class PartitionerWordPerDoc extends Partitioner<WordDocnameWritable, IntWritable> {

	@Override
	public int getPartition(WordDocnameWritable key, IntWritable arg1, int numPartition) {
		String naturalKey = key.getDocId().toString() ;
		return naturalKey.hashCode() % numPartition;
	}

}
