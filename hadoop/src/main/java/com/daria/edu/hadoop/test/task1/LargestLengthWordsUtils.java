package com.daria.edu.hadoop.test.task1;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

/**
 * Util class
 * @author Daria_Serkova
 */
public class LargestLengthWordsUtils {

	
	/**
	 * Getting max value of words's length
	 */
	public static int getMaxLength(List<String> words) {
		int maxLength = 0;
		if (CollectionUtils.isNotEmpty(words)) {
			for (String word : words) {
				int currentLength = word.length();
				if (currentLength > maxLength) {
					maxLength = currentLength;
				}
			}
		}
		return maxLength;
	}

	/**
	 * Filtering words
	 * @return list of words with max length.
	 */
	public static List<String> filter(List<String> fullWords, int maxLength) {
		MaxLengthPredicate maxLengthFilter = new MaxLengthPredicate(maxLength);
		List<String> filtered = new ArrayList<String>();
		CollectionUtils.select(fullWords, maxLengthFilter, filtered);
		return filtered;
	}

	/**
	 * Predicate for collection filtering (by max string length)
	 */
	static class MaxLengthPredicate implements Predicate {
		private int length;

		public MaxLengthPredicate(int length) {
			this.length = length;
		}

		public boolean evaluate(Object object) {
			String value = (String) object;
			return value.length() == length;
		}
	}
}