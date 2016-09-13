package org.irgroup.spark.ml;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ ModelVectorHeapEntry.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 5.
 * @Version : 1.0
 */

public class ModelVectorHeapEntry implements Comparable{
	String word;
	double key;

	public ModelVectorHeapEntry(double key, String word)
	{
		this.word = word;
		this.key = key;
	}

	@Override
	public int compareTo(Object o) {
		ModelVectorHeapEntry target = (ModelVectorHeapEntry) o;

		double ret = key - target.key;
		if(ret < 0)
			return -1;
		else if(ret > 0)
			return 1;

		return 0;
	}

	@Override
	public String toString() {
		return "[" +
				"data = '" + word + '\'' +
				", key = " + key +
				']';
	}
}
