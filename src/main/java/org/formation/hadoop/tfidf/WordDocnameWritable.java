package org.formation.hadoop.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordDocnameWritable implements WritableComparable<WordDocnameWritable> {
		private Text mot = new Text();
		private Text docId = new Text();

	public void readFields(DataInput arg0) throws IOException {
		mot.readFields(arg0);
		docId.readFields(arg0);		
	}

	public void write(DataOutput arg0) throws IOException {
		mot.write(arg0);
		docId.write(arg0);
	}

	public int compareTo(WordDocnameWritable o) {
		int compare = this.getDocId().compareTo(o.getDocId());
		if (compare != 0) {
			return compare;
		}
		return this.getMot().compareTo(o.getMot());
	}

	public Text getMot() {
		return mot;
	}

	public void setMot(Text mot) {
		this.mot = mot;
	}

	public Text getDocId() {
		return docId;
	}

	public void setDocId(Text docId) {
		this.docId = docId;
	}

	@Override
	public String toString() {
		return docId+" "+mot;
	}
	
	
	
}
