package com.daria.edu.hadoop.test.task1;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.daria.edu.hadoop.HdfsUtils;
import com.daria.edu.hadoop.test.task3.ShoppingBasketDriver;


public class LargestLengthWordsDriver extends Configured implements Tool {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ShoppingBasketDriver.class);
	
	public static void main(final String[] args) throws Exception {
		HdfsUtils.removeFolder(LargestLengthWordsConstants.OUTPUT_LOCAL_FOLDER, true);		
		int res = ToolRunner.run(new Configuration(), new LargestLengthWordsDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {	
		Date startTime = new Date();
		LOG.info("Job {} starting: {}", LargestLengthWordsConstants.JOB_NAME, startTime);
		Job job = configureJob();
		int result = job.waitForCompletion(true) ? 0 : 1;
		Date endTime = new Date();
		LOG.info("Job {} stopped: {}", LargestLengthWordsConstants.JOB_NAME, endTime);
		LOG.info("The job took {} seconds.", (endTime.getTime() - startTime.getTime()) / 1000);
		return result;
	}	
	
	private Job configureJob() throws IOException{
		JobConf config = new JobConf();
		Job job = Job.getInstance(config);
		job.setJobName(LargestLengthWordsConstants.JOB_NAME);
		job.setJarByClass(LargestLengthWordsDriver.class);
		FileInputFormat.addInputPath(job, new Path(LargestLengthWordsConstants.INPUT_LOCAL_FOLDER));		
		FileOutputFormat.setOutputPath(job, new Path(LargestLengthWordsConstants.OUTPUT_LOCAL_FOLDER));
		job.setMapperClass(LargestLengthWordsMapper.class);
		job.setReducerClass(LargestLengthWordsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		return job;
	}
}
