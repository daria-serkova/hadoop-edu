package com.daria.edu.hadoop.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.daria.edu.hadoop.HdfsUtils;

/**
 * Used for counting words in the text file 
 */
public class WordCountLocalRunner {

	public static void main(final String[] args) throws Exception {		
		HdfsUtils.removeFolder(WordCountConstants.OUTPUT_LOCAL_FOLDER, true);		
		JobConf config = new JobConf();
		Job job = Job.getInstance(config);
		job.setJobName(WordCountConstants.JOB_NAME);
		job.setJarByClass(WordCountLocalRunner.class);
		FileInputFormat.addInputPaths(job, StringUtils.join(WordCountConstants.INPUT_LOCAL_FILES, ","));
		FileOutputFormat.setOutputPath(job, new Path(WordCountConstants.OUTPUT_LOCAL_FOLDER));
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}