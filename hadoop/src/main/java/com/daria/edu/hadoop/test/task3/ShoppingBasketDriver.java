package com.daria.edu.hadoop.test.task3;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * @author Daria_Serkova 
 */
public class ShoppingBasketDriver extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(ShoppingBasketDriver.class);

	public static void main(final String[] args) throws Exception {
		HdfsUtils.removeFolder(ShoppingBasketConstants.OUTPUT_LOCAL_FOLDER, true);
		int res = ToolRunner.run(new Configuration(), new ShoppingBasketDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Date startTime = new Date();
		LOG.info("Job {} starting: {}", ShoppingBasketConstants.JOB_NAME, startTime);
		Job job = configureJob();
		int result = job.waitForCompletion(true) ? 0 : 1;
		Date endTime = new Date();
		LOG.info("Job {} stopped: {}", ShoppingBasketConstants.JOB_NAME, endTime);
		LOG.info("The job took {} seconds.", (endTime.getTime() - startTime.getTime()) / 1000);
		return result;
	}

	private Job configureJob() throws IOException {
		JobConf config = new JobConf();
		Job job = Job.getInstance(config);
		job.setJobName(ShoppingBasketConstants.JOB_NAME);
		job.setJarByClass(ShoppingBasketDriver.class);
		FileInputFormat.addInputPath(job, new Path(
				ShoppingBasketConstants.INPUT_LOCAL_FOLDER));
		FileOutputFormat.setOutputPath(job, new Path(
				ShoppingBasketConstants.OUTPUT_LOCAL_FOLDER));
		job.setMapperClass(ShoppingBasketMapper.class);
		job.setReducerClass(ShoppingBasketReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		return job;
	}
}