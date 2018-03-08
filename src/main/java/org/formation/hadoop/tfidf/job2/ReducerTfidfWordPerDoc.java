package org.formation.hadoop.tfidf.job2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.tfidf.WordDocnameWritable;
import org.formation.hadoop.tfidf.WordcountWordperdocWritable;
import org.formation.hadoop.tfidf.App.COUNTERS;

public class ReducerTfidfWordPerDoc extends Reducer<WordDocnameWritable, IntWritable, WordDocnameWritable, WordcountWordperdocWritable>{
	Map<WordDocnameWritable, IntWritable> motsNboccurrenceMap = new HashMap<WordDocnameWritable, IntWritable>();
	
	WordDocnameWritable wordDocname = new WordDocnameWritable();
	WordcountWordperdocWritable wordcountWordperdoc = new WordcountWordperdocWritable();
	
	protected void reduce(WordDocnameWritable cle, Iterable<IntWritable> valeurs, Context context) throws IOException, InterruptedException {
		motsNboccurrenceMap = new HashMap<WordDocnameWritable, IntWritable>();
		int count = 0;
		
		// Calcul du nombre total de mots dans chaque document
		// Mise en mémoire de chaque mot et le nombre d'occurence
		for(IntWritable value : valeurs) {
			System.out.println(cle);
			// Attention aux références ! Même référence, seul le contenu change
			motsNboccurrenceMap.put(new WordDocnameWritable(new Text(cle.getMot()), new Text(cle.getDocId())), new IntWritable(value.get()));
			count += value.get();
			context.getCounter(COUNTERS.NB_LINES_INPUT_JOB2).increment(1);
		}
		
		// Nombre total de mot dans un document
		wordcountWordperdoc.setWordPerDoc(new IntWritable(count));
		
		// Pour chaque mot en mémoire recréer la sortie sousforme <K,V> avec K : WordDocnameWritable (filename, word) et V : WordcountWordperdocWritable (wordcount, wordperdoc)
		for(Map.Entry<WordDocnameWritable, IntWritable> entry : motsNboccurrenceMap.entrySet()) {
			wordDocname.set(entry.getKey());
			
			wordcountWordperdoc.setWordCount(entry.getValue());
			context.write(wordDocname, wordcountWordperdoc);
			context.getCounter(COUNTERS.NB_LINES_OUTPUT_JOB2).increment(1);
		}

	}
}
