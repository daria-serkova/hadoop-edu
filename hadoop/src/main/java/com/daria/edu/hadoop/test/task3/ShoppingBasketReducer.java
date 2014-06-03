package com.daria.edu.hadoop.test.task3;

import java.io.IOException;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Daria_Serkova 
 */
public class ShoppingBasketReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	protected void reduce(IntWritable id, Iterable<Text> recommendedIds,
			Context context)
			throws IOException, InterruptedException {		
		Map<Integer, Integer> map = getRecommendedIds(recommendedIds);
		Map<Integer, Integer> sorted = sortByCount(map);
		if (sorted.size() != 0){
			Text outputValue = new Text(createRecommendedOutput(sorted));
			context.write(id, outputValue);
		}
	}
	
	/**
	 * Getting map with: 
	 * key = ID recommended for given currentID; 
	 * value = how many times this recommendedID occurs for given currentID 
	 */
	private Map<Integer, Integer> getRecommendedIds(Iterable<Text> recommendedIds){
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		for (Text data : recommendedIds) {
			String[] idsValues = StringUtils.split(data.toString(), ShoppingBasketConstants.VALUE_SEPARATOR);
			if (idsValues != null){
				for (String id : idsValues) {
					int count = 1;
					int value = Integer.parseInt(id);
					if (map.containsKey(value)){
						count = map.get(value) + 1;
					}
					map.put(value, count);
				}
			}
		}		
		return map;
	}
	
	/**
	 * Sorting map by values (by counts of recomendedId) 
	 */
	private Map<Integer, Integer> sortByCount(Map<Integer, Integer> map) {
		List<Entry<Integer, Integer>> list = new LinkedList<Entry<Integer, Integer>>(map.entrySet());
		Collections.sort(list,
				new Comparator<Entry<Integer, Integer>>() {
					public int compare(Entry<Integer, Integer> o1,
							Entry<Integer, Integer> o2) {						
						if (o2.getValue() == o1.getValue()){
							return o1.getKey().compareTo(o2.getKey());
						}
						return o2.getValue().compareTo(o1.getValue()) ;
					}
				});
		Map<Integer, Integer> sortedHashMap = new LinkedHashMap<Integer, Integer>();
		for (Iterator<Entry<Integer, Integer>> it = list.iterator(); it.hasNext();) {
			Entry<Integer, Integer> entry = it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}
	
	/**
	 * Creating output string. 
	 */
	private String createRecommendedOutput(Map<Integer, Integer> map){
		int index = 0;
		String result = StringUtils.EMPTY;
		for (Iterator<Integer> it = map.keySet().iterator(); it
				.hasNext() && index < ShoppingBasketConstants.COUNT_FOR_OUTPUT;) {
			Integer id = it.next();
			Integer count = map.get(id);
			result += id + "(" + count + ")" + ShoppingBasketConstants.VALUE_SEPARATOR;
			index++;
		}
		return result;
	}	
}