package org.irgroup.spark.ml;

import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.irgroup.datastructure.MaxHeap;
import org.irgroup.scala.util.PrahaMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.WrappedArray;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ ExampleCmdLauncher.java
 * </pre>
 * <p>
 * 
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 7.
 * @Version : 1.0
 */

public class ExampleCmdLauncher
{

	private static final Logger	logger	= LoggerFactory.getLogger(ExampleCmdLauncher.class);

	Word2VecExample				word2Vec;
	KMeanExample				kmean;

	public ExampleCmdLauncher(String dataDir)
	{
		word2Vec = new Word2VecExample(dataDir);
		kmean = new KMeanExample(1000, dataDir);

		word2Vec.load();
		kmean.load();
	}

	public void featureCount()
	{
		int totalCount = 0;

		List<Row> row = word2Vec.featureDF.collectAsList();
		HashMap<String, Integer> map = new HashMap();

		for (Row r : row)
		{
			WrappedArray<String> was = r.getAs("terms");

			for (int i = 0; i < was.size(); i++)
			{
				String ss = was.apply(i);
				map.put(ss, 1);

				totalCount++;
			}
		}

		System.out.println("row count         = " + row.size());
		System.out.println("total term count  = " + totalCount);
		System.out.println("unique term count = " + map.size());
	}

	private void printKMean()
	{
		kmean.documentVectorDF.show(false);

		System.out.println("[model info]");
		Vector[] centers = kmean.model.clusterCenters();
		System.out.println("center number = " + centers.length);

		int i = 0;
		for (Vector center : centers)
		{
			System.out.println(center);
			if (++i >= 20)
			{
				System.out.println("only showing top 20 rows");
				break;
			}
		}

		System.out.println("[cluster summary]");

		if (kmean.model.hasSummary())
		{
			KMeansSummary summary = kmean.model.summary();

			System.out.println("transformed cluster...");
			summary.cluster().show(false);

			System.out.println("predictions ...");
			summary.predictions().show(false);

			System.out.println("Summary's K = " + summary.k());

			System.out.println("Cluster size...(" + summary.clusterSizes().length + ")");
			long[] cs = summary.clusterSizes();
			i = 0;
			for (long e : cs)
			{
				System.out.println(e);
				if (++i >= 20)
				{
					System.out.println("only showing top 20 rows");
					break;
				}
			}
		}
		else
		{
			System.out.println("Summary is not exists.");

			if (kmean.clusteringResultDF != null)
			{
				System.out.println("clusteringResultDF show..");
				kmean.clusteringResultDF.show(false);
			}

		}
	}

	public void statistics()
	{
		// word2Vec statistics
		System.out.println("[feature info]");
		featureCount();

		System.out.println("\n[word2Vec info]");
		System.out.println("word vector count = " + word2Vec.getVectors().count());
		System.out.println("document vector count = " + word2Vec.getDocumentVectors().count());

		System.out.println("\n[kmean info]");
		printKMean();
	}

	public void statisticsWord()
	{
		System.out.println("[feature info]");
		featureCount();
	}

	public void statisticsDocument()
	{
		System.out.println("[document info]");
		System.out.println("document vector count = " + word2Vec.getDocumentVectors().count());
	}

	public void statisticsWord2Vec()
	{
		System.out.println("\n[word2Vec info]");
		System.out.println("word vector count = " + word2Vec.getVectors().count());
	}

	public void statisticsCluster()
	{
		System.out.println("\n[cluster info]");
		printKMean();
	}

	public void printWordVector(String word)
	{
		printWordVector(word, 20);
	}

	public void printWordVector(String word, int count)
	{
		Dataset<Row> vectors = word2Vec.getWordVector(word, true);
		vectors.show(count, false);
	}

	public void printDocumentVector(int id)
	{
		logger.info("printDocumentVector");

		Vector v = word2Vec.getDocumentVector(id);
		System.out.println("document id = " + id);

		assert v != null : id + " is not exists";

		System.out.println(v.toString());
	}

	public void printClusterVector(int cid)
	{
		Vector v = kmean.getClusterCenter(cid);
		if (v != null)
			System.out.println(v);
	}

	public void findSynonym(String word, int count)
	{
		logger.info("findSynonym");

		Dataset<Row> synonym = word2Vec.findSynonym(word, count);
		if (synonym != null)
		{
			synonym.show(count, false);
		}
		else
		{
			System.out.println(word + "not found..");
		}
	}

	public void printWord2WordSimilarity(String word, int count)
	{
		logger.info("printWord2WordSimilarity");

		// get word vector
		Dataset<Row> tempDF = word2Vec.getWordVector(word, false);
		if (tempDF == null)
		{
			System.out.println("\"" + word + "\" is not exists");
			return;
		}

		Row wordVector = tempDF.collectAsList().get(0);
		List<Row> vectors = word2Vec.getVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(wordVector.getAs("vector"), row.getAs("vector"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("word")));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printWord2DocSimilarity(String word, int count)
	{
		logger.info("printWord2DocSimilarity");

		// get word vector
		Dataset<Row> tempDF = word2Vec.getWordVector(word, false);
		if (tempDF == null)
		{
			System.out.println("\"" + word + "\" is not exists");
			return;
		}

		Row wordVector = tempDF.collectAsList().get(0);
		List<Row> vectors = word2Vec.getDocumentVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(wordVector.getAs("vector"), row.getAs("result"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("id") + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printWord2ClusterSimilarity(String word, int count)
	{
		// get word vector
		Dataset<Row> tempDF = word2Vec.getWordVector(word, false);
		if (tempDF == null)
		{
			System.out.println("\"" + word + "\" is not exists");
			return;
		}

		Row wordVector = tempDF.collectAsList().get(0);
		Vector[] vectors = kmean.model.clusterCenters();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Vector vector : vectors)
		{
			double distance = PrahaMath.euclideanDistance(wordVector.getAs("vector"), vector);

			maxHeap.insert(new ModelVectorHeapEntry(distance, i++ + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printDoc2WordSimilarity(int docid, int count)
	{
		// get doc vector
		Vector docVector = word2Vec.getDocumentVector(docid);
		if (docVector == null)
		{
			System.out.println("\"" + docid + "\" is not exists");
			return;
		}

		List<Row> vectors = word2Vec.model.getVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(docVector, row.getAs("vector"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("word")));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printDoc2DocSimilarity(int docid, int count)
	{
		// get doc vector
		Vector docVector = word2Vec.getDocumentVector(docid);
		if (docVector == null)
		{
			System.out.println("\"" + docid + "\" is not exists");
			return;
		}

		List<Row> vectors = word2Vec.documentVectorDF.select("id", "result").collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(docVector, row.getAs("result"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("id") + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printDoc2ClusterSimilarity(int docid, int count)
	{
		// get document vector
		Vector docVector = word2Vec.getDocumentVector(docid);
		if (docVector == null)
		{
			System.out.println("\"" + docid + "\" is not exists");
			return;
		}

		Vector[] vectors = kmean.model.clusterCenters();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Vector vector : vectors)
		{
			double distance = PrahaMath.euclideanDistance(docVector, vector);

			maxHeap.insert(new ModelVectorHeapEntry(distance, i++ + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printCluster2WordSimilarity(int clusterId, int count)
	{
		// get cluster vector
		Vector clusterVector = kmean.getClusterCenter(clusterId);
		if (clusterVector == null)
		{
			System.out.println("\"" + clusterId + "\" is not exists");
			return;
		}

		List<Row> vectors = word2Vec.model.getVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(clusterVector, row.getAs("vector"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("word")));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printCluster2DocSimilarity(int clusterId, int count)
	{
		// get cluster vector
		Vector clusterVector = kmean.getClusterCenter(clusterId);
		if (clusterVector == null)
		{
			System.out.println("\"" + clusterId + "\" is not exists");
			return;
		}

		List<Row> vectors = word2Vec.documentVectorDF.select("id", "result").collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(clusterVector, row.getAs("result"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("id") + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void printCluster2ClusterSimilarity(int clusterId, int count)
	{
		// get cluster vector
		Vector clusterVector = kmean.getClusterCenter(clusterId);
		if (clusterVector == null)
		{
			System.out.println("\"" + clusterId + "\" is not exists");
			return;
		}

		Vector[] vectors = kmean.model.clusterCenters();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		int i = 0;
		for (Vector vector : vectors)
		{
			double distance = PrahaMath.euclideanDistance(clusterVector, vector);

			maxHeap.insert(new ModelVectorHeapEntry(distance, i++ + ""));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
	}

	public void predict(String condition, String relation, String question, int count)
	{
		Row c = word2Vec.getWordVector(condition, false).collectAsList().get(0);
		Row r = word2Vec.getWordVector(relation, false).collectAsList().get(0);
		Row q = word2Vec.getWordVector(question, false).collectAsList().get(0);

		double[] dc = ((Vector)c.getAs("vector")).toArray();
		double[] dr = ((Vector)r.getAs("vector")).toArray();
		double[] dq = ((Vector)q.getAs("vector")).toArray();

		System.out.println(String.format("condition : %s (%d)", c.getAs("word"), dc.length));
		System.out.println(String.format("relation  : %s (%d)", r.getAs("word"), dr.length));
		System.out.println(String.format("question  : %s (%d)", q.getAs("word"), dq.length));

		double[] tempVector = new double[dc.length];

		for(int i = 0; i < dc.length; i ++)
		{
			tempVector[i] = dc[i] - dr[i] + dq[i];
		}

		Vector predictVector = new DenseVector(tempVector);
		List<Row> vectors = word2Vec.getVectors().collectAsList();

		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
		MaxHeap maxHeap = new MaxHeap(entries, 0, count);
		for (Row row : vectors)
		{
			double distance = PrahaMath.euclideanDistance(predictVector, row.getAs("vector"));

			maxHeap.insert(new ModelVectorHeapEntry(distance, row.getAs("word")));
		}

		while (maxHeap.count() > 0)
		{
			ModelVectorHeapEntry e = (ModelVectorHeapEntry) maxHeap.remove();
			System.out.println(e.toString());
		}
//
//		ModelVectorHeapEntry[] entries = (ModelVectorHeapEntry[]) Array.newInstance(ModelVectorHeapEntry.class, count + 1);
//		MinHeap minHeap = new MinHeap(entries, 0, count);
//		for (Row row : vectors) {
//			String word = row.getAs("word");
//			double cur = PrahaMath.cosineSimilarity(predictVector, row.getAs("vector"));
//
//			minHeap.insert(new ModelVectorHeapEntry(cur, word));
//		}
//		minHeap.build();
//
//		while (minHeap.count() > 0)
//		{
//			ModelVectorHeapEntry e = (ModelVectorHeapEntry) minHeap.remove();
//			System.out.println(e.toString());
//		}
	}
}
