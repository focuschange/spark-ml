package org.irgroup.spark.ml;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Row;
import org.irgroup.datastructure.MaxHeap;
import org.irgroup.datastructure.MinHeap;
import org.irgroup.scala.util.PrahaMath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ Word2VecTest.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 8. 31.
 * @Version : 1.0
 */

public class Word2VecTest {
	private Word2VecExample word2Vec;
	private String dataDir = "data";

	@Before
	public void setUp() throws Exception {
		word2Vec = new Word2VecExample(dataDir);
	}

	@After
	public void tearDown() throws Exception {
		word2Vec.stop();
	}

	@Test
	public void run() throws Exception {
		word2Vec.run(null);
	}

	@Test
	public void findSynonym() throws Exception {
		word2Vec.load();
		//word2Vec.findSynonym("반팔티", 10).show(false);
		word2Vec.findSynonym("2단자동", 10).show(false);
		word2Vec.getVectors().show(false);
	}

	@Test
	public void featureCount()
	{
		word2Vec.load();

		int totalCount = 0;

		List<Row> row = word2Vec.featureDF.collectAsList();
		HashMap<String, Integer> map = new HashMap();

		for (Row r : row) {
			WrappedArray<String> was = r.getAs("termskma");

			for (int i = 0; i < was.size(); i++) {
				String ss = was.apply(i);
				map.put(ss, 1);

				totalCount++;
			}
		}

		System.out.println("row count         = " + row.size());
		System.out.println("total term count  = " + totalCount);
		System.out.println("unique term count = " + map.size());
	}

	@Test
	public void statistics() throws Exception {
		featureCount();

		System.out.println("featureDF show..");
		word2Vec.featureDF.show(false);

		assert word2Vec.model != null : "model is null";
		System.out.println("word vector count = " + word2Vec.model.getVectors().count());
		System.out.println("word vector show..");
		word2Vec.model.getVectors().show(false);

		assert word2Vec.documentVectorDF != null : "documentVectorDF is null";
		System.out.println("document vector size = " + word2Vec.documentVectorDF.count());
		System.out.println("documentVectorDF show..");
		word2Vec.documentVectorDF.show(false);


	}

	@Test
	public void printWordVector() throws Exception {
		word2Vec.load();
		word2Vec.getWordVector("2단자동", true).show(false);
	}

	@Test
	public void printDocumentVector() throws Exception {
		word2Vec.load();
		word2Vec.getDocument(1302884).show(false);
	}

	@Test
	public void printDealWord2VecSimilarity() throws Exception {
		int id = 1302884;
		int count = 10;

		word2Vec.load();
		Vector docVector = word2Vec.getDocumentVector(id);

		List<Row> modelVectors = word2Vec.getVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		double cur;

		for (Row row : modelVectors) {
			String word = row.getAs("word");
			cur = PrahaMath.euclideanDistance(docVector, row.getAs("vector"));

			maxHeap.insert(new ModelVectorHeapEntry(cur, word));
		}
		maxHeap.build();

		entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MinHeap minHeap = new MinHeap(entries, 0, count);
		for (Row row : modelVectors) {
			String word = row.getAs("word");
			cur = PrahaMath.cosineSimilarity(docVector, row.getAs("vector"));

			minHeap.insert(new ModelVectorHeapEntry(cur, word));
		}
		minHeap.build();

		int i = 0;
		while (minHeap.count() > 0 || maxHeap.count() > 0) {
			String msg = i++ + ") [euclidean] ";

			if (maxHeap.count() > 0) {
				ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
				msg += e.toString() + " ";
			}

			msg += "[cosine] ";
			if (minHeap.count() > 0) {
				ModelVectorHeapEntry e = (ModelVectorHeapEntry) minHeap.remove();
				msg += e.toString();
			}

			System.out.println(msg);
		}
	}

	@Test
	public void loadRawData() throws Exception {
		word2Vec.loadRawData(dataDir + "/terms.txt");
		assertNotNull(word2Vec.featureDF);
		word2Vec.featureDF.show(false);
	}
}