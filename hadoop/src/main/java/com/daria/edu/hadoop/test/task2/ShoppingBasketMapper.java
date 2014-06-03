package com.daria.edu.hadoop.test.task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Daria_Serkova 
 */
public class ShoppingBasketMapper extends Mapper<LongWritable, Text, Text, ShoppingBasketPair>{
	
	private final Text outputKey = new Text("pair");
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {		
		
		String dataLine = value.toString();
		List<String> ids = getIds(dataLine);
		List<ShoppingBasketPair> pairs = getPairs(ids);
		if (CollectionUtils.isNotEmpty(pairs)){
			for (ShoppingBasketPair shoppingBasketPair : pairs) {
				context.write(outputKey, shoppingBasketPair);
			}
		}	
	}	
	
	/**
	 * Getting pairs for each ID from input line.
	 */
	private List<ShoppingBasketPair> getPairs(List<String> ids){
		if (CollectionUtils.isEmpty(ids)){
			return null;			
		}
		List<ShoppingBasketPair> pairs = new ArrayList<ShoppingBasketPair>();		
		for(int i=0 ; i < ids.size(); i++){
			for(int j=i+1 ; j < ids.size(); j++){
				ShoppingBasketPair pair = new ShoppingBasketPair(Integer.parseInt(ids.get(i)), Integer.parseInt(ids.get(j)));
				pairs.add(pair);
			}
		}
		return pairs;
	}
	
	/**
	 * Getting list of IDs from input line
	 */
	private List<String> getIds(String dataLine){
		if (StringUtils.isBlank(dataLine)){
			return null;
		}	
		return Arrays.asList(StringUtils.split(dataLine, ShoppingBasketConstants.VALUE_SEPARATOR));	
	}	
}