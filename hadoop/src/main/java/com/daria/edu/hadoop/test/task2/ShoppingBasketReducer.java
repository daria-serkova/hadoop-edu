package com.daria.edu.hadoop.test.task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Daria_Serkova 
 */
public class ShoppingBasketReducer extends
		Reducer<Text, ShoppingBasketPair, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<ShoppingBasketPair> pairs,
			Context context) throws IOException, InterruptedException {

		List<ShoppingBasketPair> list = convertToList(pairs);
		Map<ShoppingBasketPair, Integer> map = countPairs(list);
		Map<ShoppingBasketPair, Integer> sorted = sort(map);
		int index = 0;
		for (Iterator<ShoppingBasketPair> it = sorted.keySet().iterator(); it
				.hasNext() && index < ShoppingBasketConstants.COUNT_FOR_OUTPUT;) {
			ShoppingBasketPair pair = it.next();
			Text outputKey = new Text(pair.toString());
			IntWritable outputValue = new IntWritable(sorted.get(pair));
			context.write(outputKey, outputValue);
			index++;
		}
	}

	private List<ShoppingBasketPair> convertToList(
			Iterable<ShoppingBasketPair> pairs) {
		List<ShoppingBasketPair> list = new ArrayList<ShoppingBasketPair>();
		for (ShoppingBasketPair shoppingBasketPair : pairs) {
			ShoppingBasketPair pair = new ShoppingBasketPair(
					shoppingBasketPair.getFirstId(),
					shoppingBasketPair.getSecondId());
			list.add(pair);
		}
		return list;
	}
	
	/**
	 * Counting pairs.
	 * @return map with key = pair; value = how many times this pair occurs in files.
	 */
	private Map<ShoppingBasketPair, Integer> countPairs(List<ShoppingBasketPair> pairs) {
		Map<ShoppingBasketPair, Integer> map = new HashMap<ShoppingBasketPair, Integer>();
		for (ShoppingBasketPair shoppingBasketPair : pairs) {
			if (!map.containsKey(shoppingBasketPair)) {
				int count = Collections.frequency(pairs, shoppingBasketPair);
				map.put(shoppingBasketPair, count);
			}
		}
		return map;
	}

	/**
	 * Sorting map by key and values 
	 */
	private Map<ShoppingBasketPair, Integer> sort(Map<ShoppingBasketPair, Integer> map) {
		List<Entry<ShoppingBasketPair, Integer>> list = new LinkedList<Entry<ShoppingBasketPair, Integer>>(map.entrySet());
		Collections.sort(list,
				new Comparator<Entry<ShoppingBasketPair, Integer>>() {
					public int compare(Entry<ShoppingBasketPair, Integer> o1,
							Entry<ShoppingBasketPair, Integer> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}
				});
		Collections.sort(list,
				new Comparator<Entry<ShoppingBasketPair, Integer>>() {
					public int compare(Entry<ShoppingBasketPair, Integer> o1,
							Entry<ShoppingBasketPair, Integer> o2) {
						return o2.getValue().compareTo(o1.getValue());
					}
				});
		
		Map<ShoppingBasketPair, Integer> sortedHashMap = new LinkedHashMap<ShoppingBasketPair, Integer>();
		for (Iterator<Entry<ShoppingBasketPair, Integer>> it = list.iterator(); it.hasNext();) {
			Entry<ShoppingBasketPair, Integer> entry = it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}
}