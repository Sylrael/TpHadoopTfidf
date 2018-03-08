package org.formation.hadoop.tfidf.job3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.tfidf.WordDocnameWritable;
import org.formation.hadoop.tfidf.WordcountWordperdocWritable;

public class ReducerTfidfCalculTfidf
		extends Reducer<WordDocnameWritable, WordcountWordperdocWritable, WordDocnameWritable, DoubleWritable> {
	DoubleWritable tfidf = new DoubleWritable();
	Map<WordDocnameWritable, WordcountWordperdocWritable> tfidfmap = new HashMap<WordDocnameWritable, WordcountWordperdocWritable>();

	WordDocnameWritable wordDocname = new WordDocnameWritable();
	WordcountWordperdocWritable wordcountWordperdoc = new WordcountWordperdocWritable();

	protected void reduce(WordDocnameWritable cle, Iterable<WordcountWordperdocWritable> valeurs, Context context)
			throws IOException, InterruptedException {
		tfidfmap = new HashMap<WordDocnameWritable, WordcountWordperdocWritable>();
		int nbDocumentTotal = 2;
		int nbDocumentPresent = 0;
		for (WordcountWordperdocWritable value : valeurs) {
			tfidfmap.put(new WordDocnameWritable(new Text(cle.getDocId()), new Text(cle.getMot())),
					new WordcountWordperdocWritable(new IntWritable(value.getWordCount().get()),
							new IntWritable(value.getWordPerDoc().get())));
			nbDocumentPresent++;
		}

		for (Map.Entry<WordDocnameWritable, WordcountWordperdocWritable> entry : tfidfmap.entrySet()) {
			wordDocname = entry.getKey();
			double freqDoc = (double)entry.getValue().getWordCount().get();
			double nbmotsDoc = (double)entry.getValue().getWordPerDoc().get();
			tfidf = new DoubleWritable(
					freqDoc / nbmotsDoc * Math.log((double) (nbDocumentTotal) / (double) nbDocumentPresent));
			context.write(wordDocname, tfidf);
		}
	}
}
