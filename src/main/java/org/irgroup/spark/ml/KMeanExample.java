package org.irgroup.spark.ml;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger	logger					= LoggerFactory.getLogger(KMeanExample.class);

	private String				dataDir;
	private String				KMeanFile				= "kmean";
	private String				documentVectorFile		= "documentVector";
	private String				clusteringResultFile	= "clusteringResultDF";

	int							kmeansK;
	KMeans						kmeans;
	KMeansModel					model;

	Dataset<Row>				documentVectorDF;
	Dataset<Row>				clusteringResultDF;

	public KMeanExample(int K, String dataDir) {
		super(KMeanExample.class.getSimpleName());

		this.dataDir = dataDir;
		KMeanFile = this.dataDir + "/" + KMeanFile;
		documentVectorFile = this.dataDir + "/" + documentVectorFile;
		clusteringResultFile = this.dataDir + "/" + clusteringResultFile;

		kmeansK = K;

		kmeans = new KMeans()
				.setFeaturesCol("result")
				.setPredictionCol("cluster")
				.setInitMode("k-means||")
				.setK(kmeansK);

		documentVectorDF = load(documentVectorFile);
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
		clusteringResultDF = load(clusteringResultFile).cache();
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
