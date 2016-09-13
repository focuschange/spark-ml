package org.irgroup.spark.ml;

import org.apache.commons.io.FileUtils;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ KMeanExample.java
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

public class KMeanExample extends SparkManager {

	private static final Logger logger = LoggerFactory.getLogger(KMeanExample.class);

	private final String KMeanFile = "./data/kmean";
	private final String documentVectorFile = "./data/documentVector";
	private final String clusteringResultFile = "./data/clusteringResultDF";

	int kmeansK;
	KMeans kmeans;
	KMeansModel model;

	Dataset<Row> documentVectorDF = load(documentVectorFile);
	Dataset<Row> clusteringResultDF;

	public KMeanExample(int K) {
		super(KMeanExample.class.getSimpleName());

		kmeansK = K;

		kmeans = new KMeans()
				.setFeaturesCol("result")
				.setPredictionCol("cluster")
				.setInitMode("k-means||")
				.setK(kmeansK);
	}

	public void fit()
	{
		model = kmeans.fit(documentVectorDF);
	}

	public void transform()
	{
		clusteringResultDF = model != null ? model.transform(documentVectorDF) : null;
	}

	public void save()
	{
		save(model, KMeanFile);
		save(clusteringResultDF, clusteringResultFile);
	}

	public void load()
	{
		model = KMeansModel.load(KMeanFile);
		clusteringResultDF = load(clusteringResultFile);
	}

	public Vector getClusterCenter(int cid)
	{
		return (cid < 0 || cid >= model.clusterCenters().length) ? null : model.clusterCenters()[cid];
	}

	@Override
	public void run(String[] args) throws Exception {
		fit();
		transform();
		save();
	}

	@Override
	public void stop() throws Exception {

	}
}
