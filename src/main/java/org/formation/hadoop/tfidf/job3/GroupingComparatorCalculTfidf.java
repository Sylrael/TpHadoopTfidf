package org.formation.hadoop.tfidf.job3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.formation.hadoop.tfidf.WordDocnameWritable;

public class GroupingComparatorCalculTfidf extends WritableComparator {

	public GroupingComparatorCalculTfidf() {
			super(WordDocnameWritable.class, true);
		}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		WordDocnameWritable key1 = (WordDocnameWritable) w1;
		WordDocnameWritable key2 = (WordDocnameWritable) w2;
		int value = key1.getMot().compareTo(key2.getMot());
		return value;
	}
}
