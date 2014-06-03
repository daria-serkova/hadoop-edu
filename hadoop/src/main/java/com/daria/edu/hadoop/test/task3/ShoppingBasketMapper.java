package com.daria.edu.hadoop.test.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.daria.edu.hadoop.test.task2.ShoppingBasketConstants;

/**
 * @author Daria_Serkova 
 */
public class ShoppingBasketMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {	
		String dataLine = value.toString();		
		List<String> ids = getIds(dataLine);
		for (String id : ids) {
			List<String> otherIds = new ArrayList<String>();
			CollectionUtils.selectRejected(ids, new RecommendedIds(id), otherIds);
			if (CollectionUtils.isNotEmpty(otherIds)){
				IntWritable outputKey = new IntWritable(Integer.parseInt(id));
				Text outputValue = new Text(StringUtils.join(otherIds, ShoppingBasketConstants.VALUE_SEPARATOR));
				context.write(outputKey, outputValue);
			}
		}
	}
	
	/**
	 * Getting list of IDS from text line. 
	 */
	private List<String> getIds(String dataLine){
		if (StringUtils.isBlank(dataLine)){
			return null;
		}
		return Arrays.asList(StringUtils.split(dataLine, ShoppingBasketConstants.VALUE_SEPARATOR));
	}
	
	
	/**
	 * Predicator for filtering currentID from list of IDS. 
	 */
	class RecommendedIds implements Predicate{
		private String currentId;
		
		public RecommendedIds(String currentId) {		
			this.currentId = currentId;
		}	
		
		public boolean evaluate(Object object) {
			String value = (String) object;			
			return StringUtils.equalsIgnoreCase(value, currentId);
		}
	}	
}