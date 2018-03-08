package org.formation.hadoop.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordDocnameWritable implements WritableComparable<WordDocnameWritable> {
	private Text mot = new Text();
	private Text docId = new Text();

	public WordDocnameWritable() {
	}

	public WordDocnameWritable(Text mot, Text docId) {
		super();
		this.mot = new Text(mot);
		this.docId = new Text(docId);
	}

	public void readFields(DataInput arg0) throws IOException {
		mot.readFields(arg0);
		docId.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		mot.write(arg0);
		docId.write(arg0);
	}
	
	public void set(WordDocnameWritable other) {
		mot = new Text(other.getMot());
		docId = new Text(other.getDocId());
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
		this.mot = new Text(mot.toString());
	}

	public Text getDocId() {
		return docId;
	}

	public void setDocId(Text docId) {
		this.docId = new Text(docId.toString());
	}

	@Override
	public String toString() {
		return docId + "\t" + mot;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((docId == null) ? 0 : docId.hashCode());
		result = prime * result + ((mot == null) ? 0 : mot.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordDocnameWritable other = (WordDocnameWritable) obj;
		if (docId == null) {
			if (other.docId != null)
				return false;
		} else if (!docId.equals(other.docId))
			return false;
		if (mot == null) {
			if (other.mot != null)
				return false;
		} else if (!mot.equals(other.mot))
			return false;
		return true;
	}

	

}
