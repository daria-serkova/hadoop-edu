package com.daria.edu.hadoop.test.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for getting list of words with biggest length.
 * @author Daria_Serkova
 */
public class LargestLengthWordsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final Text outputKey = new Text("maxWord");

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {	
		String dataLine = value.toString();
		List<String> words = getWords(dataLine);
		int maxLength = LargestLengthWordsUtils.getMaxLength(words);
		List<String> maxWords = LargestLengthWordsUtils.filter(words, maxLength);
		if (CollectionUtils.isNotEmpty(maxWords)) {
			for (String maxWord : maxWords) {
				context.write(outputKey, new Text(maxWord));
			}
		}
	}

	/**
	 * Getting list of words from text line (without duplicates).
	 * @param text - line from text file.	 
	 */
	private List<String> getWords(String text) {
		if (StringUtils.isNotBlank(text)) {
			String[] data = StringUtils.split(text, LargestLengthWordsConstants.SPLIT_WORDS_REGEX);
			if (data != null) {
				List<String> words = new ArrayList<String>();
				for (String word : data) {
					if (StringUtils.isNotBlank(word) && !words.contains(word)) {
						words.add(word);
					}
				}
				return words;
			}
		}
		return null;
	}
}