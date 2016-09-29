package org.irgroup.spark.ml;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ SparkManager.java
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

public abstract class SparkManager implements Serializable {

	private static final long serialVersionUID = 5277049394470857228L;
	private static final Logger logger = LoggerFactory.getLogger(SparkManager.class);

	public SparkConf sparkConf;
	public SparkSession sparkSession;

	/**
	 * Constructor
	 *
	 * @param appName Driver Name
	 */
	public SparkManager(String appName) {

		sparkConf = new SparkConf()
				.setAppName(appName + "_" + System.currentTimeMillis())
				.setMaster("local")
				.set("spark.executor.cores", "2")
				.set("spark.executor.memory", "8g")
				.set("spark.driver.cores", "2")
				.set("spark.driver.memory", "2g")
				.set("spark.eventLog.enabled", "false")
		//.set("spark.eventLog.dir", "")
		//.setJars(new String[]{"~/.m2/repository/org/apache/hadoop/hadoop-yarn-api/2.7.2/hadoop-yarn-api-2.7.2.jar"})
		;

		sparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();

		printRuntimeEnv();
	}

	public void printRuntimeEnv()
	{
		logger.info("Spark Driver Configurations [\n{}\n]", sparkConf.toDebugString());
		logger.info("Spark Driver Runtime Environment...");
		scala.collection.immutable.HashMap.HashTrieMap<String, String> map = (scala.collection.immutable.HashMap.HashTrieMap<String, String>) sparkSession.conf().getAll();

		Iterator<Tuple2<String, String>> iterator = map.iterator();

		while(iterator.hasNext())
		{
			Tuple2<String, String> tuple = iterator.next();

			System.out.println(tuple.toString());
		}
	}

	public Dataset<Row> load(String path)
	{
		return sparkSession.read().load(path);
	}

	public void save(Dataset<Row> data, String path)
	{
		try {
			FileUtils.deleteDirectory(new File(path));
			data.write().mode(SaveMode.ErrorIfExists).save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void save(Word2VecModel data, String path)
	{
		try {
			FileUtils.deleteDirectory(new File(path));
			data.save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void save(KMeansModel data, String path)
	{
		try {
			FileUtils.deleteDirectory(new File(path));
			data.write().save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public abstract void run(String[] args) throws Exception;

	public abstract void stop() throws Exception;
}
