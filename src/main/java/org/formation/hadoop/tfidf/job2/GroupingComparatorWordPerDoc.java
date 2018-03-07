package org.formation.hadoop.tfidf.job2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.formation.hadoop.tfidf.WordDocnameWritable;

public class GroupingComparatorWordPerDoc extends WritableComparator {
	
	public GroupingComparatorWordPerDoc() {
		super(WordDocnameWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
	    WordDocnameWritable key1 = (WordDocnameWritable) w1;
	    WordDocnameWritable key2 = (WordDocnameWritable) w2;
	    int value = key1.getDocId().compareTo(key2.getDocId());
	    return value;
	}
}
