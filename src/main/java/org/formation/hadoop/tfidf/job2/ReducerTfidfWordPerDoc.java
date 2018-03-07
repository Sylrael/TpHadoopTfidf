package org.formation.hadoop.tfidf.job2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.tfidf.WordDocnameWritable;
import org.formation.hadoop.tfidf.WordcountWordperdocWritable;

public class ReducerTfidfWordPerDoc extends Reducer<WordDocnameWritable, IntWritable, WordDocnameWritable, WordcountWordperdocWritable>{
	Map<Text, IntWritable> motsNboccurrenceMap = new HashMap<Text, IntWritable>();
	
	WordDocnameWritable wordDocname = new WordDocnameWritable();
	WordcountWordperdocWritable wordcountWordperdoc = new WordcountWordperdocWritable();
	
	protected void reduce(WordDocnameWritable cle, Iterable<IntWritable> valeurs, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		// Calcul du nombre total de mots dans chaque document
		// Mise en mémoire de chaque mot et le nombre d'occurence
		for(IntWritable value : valeurs) {
			System.out.println(cle);
			// Attention aux références !
			motsNboccurrenceMap.put(new Text(cle.getMot()), new IntWritable(value.get()));
			count += value.get();
		}
		
		// Nombre total de mot dans un document
		wordcountWordperdoc.setWordPerDoc(new IntWritable(count));
		
		// Pour chaque mot en mémoire recréer la sortie sousforme <K,V> avec K : WordDocnameWritable (filename, word) et V : WordcountWordperdocWritable (wordcount, wordperdoc)
		for(Map.Entry<Text, IntWritable> entry : motsNboccurrenceMap.entrySet()) {
			wordDocname.setMot(entry.getKey());
			wordDocname.setDocId(cle.getDocId());
			
			wordcountWordperdoc.setWordCount(entry.getValue());
			context.write(wordDocname, wordcountWordperdoc);
		}

	}
}
