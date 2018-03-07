package org.formation.hadoop.tfidf.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.formation.hadoop.tfidf.WordDocnameWritable;
import org.formation.hadoop.tfidf.WordcountWordperdocWritable;

public class PartitionerCalculTfidf extends Partitioner<WordDocnameWritable, WordcountWordperdocWritable> {

	@Override
	public int getPartition(WordDocnameWritable key, WordcountWordperdocWritable value, int nbPartition) {
		String naturalKey = key.getDocId().toString() ;
		return naturalKey.hashCode() % nbPartition;
	}

}
