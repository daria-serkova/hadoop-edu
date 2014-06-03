package com.daria.edu.hadoop.test.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for getting list of words with biggest length.
 * @author Daria_Serkova
 */
public class LargestLengthWordsReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> wordsWithMaxLength,
			Context context) throws IOException, InterruptedException {
		List<String> wordList = convertToList(wordsWithMaxLength);
		int maxLength = LargestLengthWordsUtils.getMaxLength(wordList);
		List<String> maxWords = LargestLengthWordsUtils.filter(wordList, maxLength);
		if (CollectionUtils.isNotEmpty(maxWords)) {
			Collections.sort(maxWords);
			for (String word : maxWords) {
				context.write(new Text(word), NullWritable.get());
			}
		}
	}

	/**
	 * Getting list of words with max length without duplicates
	 * @param wordsWithMaxLength - words returned by map phase. 
	 */
	private List<String> convertToList(Iterable<Text> wordsWithMaxLength) {
		List<String> words = new ArrayList<String>();
		for (Text wordWithMaxLength : wordsWithMaxLength) {
			String value = wordWithMaxLength.toString();
			if (!words.contains(value)) {
				words.add(value);
			}
		}
		return words;
	}
}