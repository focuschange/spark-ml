package org.irgroup.spark.ml;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
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

	private int					kmeansK;
	private int					kmeansMaxIteration;

	public KMeans				kmeans;
	public KMeansModel			model;

	public Dataset<Row>			documentVectorDF;
	public Dataset<Row>			clusteringResultDF;

	public KMeanExample(String configFile)
	{
		super(KMeanExample.class.getSimpleName(), configFile);

		dataDir = getDataPath();
		KMeanFile = dataDir + "/" + KMeanFile;
		documentVectorFile = this.dataDir + "/" + documentVectorFile;
		clusteringResultFile = this.dataDir + "/" + clusteringResultFile;

		kmeansK = Integer.parseInt(getProperty(SparkManager.KMEANS_K));
		kmeansMaxIteration = Integer.parseInt(getProperty(SparkManager.KMEANS_ITERATION_MAX));

		kmeans = new KMeans()
				.setFeaturesCol("result")
				.setPredictionCol("cluster")
				.setInitMode("k-means||")
				.setK(kmeansK)
				.setMaxIter(kmeansMaxIteration)
			;

		documentVectorDF = load(documentVectorFile).cache();
	}

	public void fit()
	{
		logger.info("kmean fit");
		Dataset<Row> tmp = documentVectorDF.repartition(1024).persist(StorageLevel.MEMORY_AND_DISK());
		model = kmeans.fit(tmp);
	}

	public void transform()
	{

		logger.info("kmean transform");
		clusteringResultDF = model != null ? model.transform(documentVectorDF) : null;
	}

	public void save()
	{
		logger.info("save : {}", KMeanFile);
		save(model, KMeanFile);

		logger.info("save : {}", clusteringResultFile);
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
		sparkSession.stop();
	}

	public static void main(String[] args) throws Exception
	{

		if (args.length < 1)
		{
			System.out.println("Usage : " + KMeanExample.class.getName() + " [configFile]");
			System.exit(0);
		}

		KMeanExample kmean = new KMeanExample(args[0]);
		kmean.run(null);
		kmean.stop();
	}
}
