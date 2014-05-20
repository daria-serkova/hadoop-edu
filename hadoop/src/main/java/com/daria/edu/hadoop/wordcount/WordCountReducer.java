package com.daria.edu.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer for counting words in the files.
 * 12-05-2014 15:07
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(WordCountReducer.class);

	private IntWritable resultCount = new IntWritable();
	
	@Override
	protected void reduce(Text word, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		LOG.debug("Started reduce for [{}].", WordCountConstants.JOB_NAME);
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		resultCount.set(count);
		context.write(word, resultCount);		
		LOG.debug("Stopped reduce for [{}].", WordCountConstants.JOB_NAME);		
	}
}