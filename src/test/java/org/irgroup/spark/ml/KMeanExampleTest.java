package org.irgroup.spark.ml;

import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.linalg.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ KMeanExampleTest.java
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

public class KMeanExampleTest {
	KMeanExample kmean;
	String dataDir = "data";

	@Before
	public void setUp() throws Exception {
		kmean = new KMeanExample(1000, dataDir);
	}

	@After
	public void tearDown() throws Exception {
		kmean.stop();
	}

	@Test
	public void run() throws Exception {
		kmean.run(null);
		print();
	}

	@Test
	public void load() throws Exception {
		kmean.load();
		print();
	}

	private void print()
	{
		kmean.documentVectorDF.show(false);

		System.out.println("[model info]");
		Vector[] centers = kmean.model.clusterCenters();
		System.out.println("center number = " + centers.length);

		int i = 0;
		for(Vector center : centers)
		{
			System.out.println(center);
			if(++i >= 20) {
				System.out.println("only showing top 20 rows");
				break;
			}
		}

		System.out.println("[cluster summary]");

		if(kmean.model.hasSummary())
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
			for(long e : cs)
			{
				System.out.println(e);
				if(++i >= 20)
				{
					System.out.println("only showing top 20 rows");
					break;
				}
			}
		}
		else
		{
			System.out.println("Summary is not exists.");

			if(kmean.clusteringResultDF != null) {
				System.out.println("clusteringResultDF show..");
				kmean.clusteringResultDF.show(false);
			}

		}
	}
}