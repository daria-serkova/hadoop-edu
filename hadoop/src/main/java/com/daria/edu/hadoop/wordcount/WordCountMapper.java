package com.daria.edu.hadoop.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper for counting words in files.
 * 
 * @author Daria_Serkova 12-05-2014 15:07
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
	private final String SPLIT_WORDS_REGEX = " ";

	private Text currentWord = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text textLine, Context context)
			throws IOException, InterruptedException {
		LOG.debug("Start map method for [{}].", WordCountConstants.JOB_NAME);
		String[] words = getWords(textLine.toString());
		if (words != null) {
			for (String word : words) {
				currentWord.set(word);
				context.write(currentWord, one);
			}
		}
		LOG.debug("Stop map method for [{}].", WordCountConstants.JOB_NAME);
	}

	private String[] getWords(String text) {
		if (StringUtils.isNotBlank(text)) {
			return text.split(SPLIT_WORDS_REGEX);
		}
		return null;
	}
}