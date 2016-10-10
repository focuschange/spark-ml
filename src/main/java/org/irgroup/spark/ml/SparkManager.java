package org.irgroup.spark.ml;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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
import scala.collection.immutable.HashMap;

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

	private static final long		serialVersionUID						= 5277049394470857228L;
	private static final Logger		logger									= LoggerFactory.getLogger(SparkManager.class);

	private String					configFile								= "/config.properties";

	private static final String		SPARK_MASTER							= "spark.master";
	private static final String		SPARK_DRIVER_CORES						= "spark.driver.cores";
	private static final String		SPARK_DRIVER_MEMORY						= "spark.driver.memory";
	private static final String		SPARK_EXECUTOR_CORES					= "spark.executor.cores";
	private static final String		SPARK_EXECUTOR_MEMORY					= "spark.executor.memory";
	private static final String		SPARK_EVENTLOG_ENABLED					= "spark.eventlog.enabled";
	private static final String		SPARK_DYNAMIC_ALLOCATION_ENABLED		= "spark.dynamicAllocation.enabled";
	private static final String		SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS	= "spark.dynamicAllocation.minExecutors";
	private static final String		SPARK_SHUFFLE_SERVICE_ENABED			= "spark.dynamicAllocation.minExecutors";

	private static final String		SPARK_MONGODB_INPUT_URI					= "spark.mongodb.input.uri";
	private static final String		SPARK_MONGODB_INPUT_DATABASE			= "spark.mongodb.input.database";
	private static final String		SPARK_MONGODB_INPUT_COLLECTION			= "spark.mongodb.input.collection";
	private static final String		SPARK_MONGODB_OUTPUT_URI				= "spark.mongodb.output.uri";
	private static final String		SPARK_MONGODB_OUTPUT_DATABASE			= "spark.mongodb.output.database";

	private static final String		DATA_DIRECTORY							= "data.directory";
	public static final String		KMEANS_K								= "kmeans.k";
	public static final String		KMEANS_ITERATION_MAX					= "kmeans.iteration.max";

	public SparkConf				sparkConf;
	public SparkSession				sparkSession;
	public PropertiesConfiguration	properties;

	/**
	 * Constructor
	 *
	 * @param appName Driver Name
	 */
	public SparkManager(String appName, String configFile) {

		this.configFile = configFile;
		try {
//			properties = new PropertiesConfiguration(getClass().getResource("/config.properties"));
			properties = new PropertiesConfiguration(this.configFile);

		} catch (ConfigurationException e) {
			logger.error(e.getMessage(),e);
		}

		sparkConf = new SparkConf()
				.setAppName(appName)
				.setMaster(properties.getString(SPARK_MASTER, "local[*]"))
				.set(SPARK_EXECUTOR_CORES,                   properties.getString(SPARK_EXECUTOR_CORES, "2"))
				.set(SPARK_EXECUTOR_MEMORY,                  properties.getString(SPARK_EXECUTOR_MEMORY, "8g"))
				.set(SPARK_DRIVER_CORES,                     properties.getString(SPARK_DRIVER_CORES, "2"))
				.set(SPARK_DRIVER_MEMORY,                    properties.getString(SPARK_DRIVER_MEMORY, "8g"))
				.set(SPARK_EVENTLOG_ENABLED,                 properties.getString(SPARK_EVENTLOG_ENABLED, "false"))
				.set(SPARK_DYNAMIC_ALLOCATION_ENABLED,       properties.getString(SPARK_DYNAMIC_ALLOCATION_ENABLED, "true"))
				.set(SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS, properties.getString(SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS, "1"))
				.set(SPARK_SHUFFLE_SERVICE_ENABED,           properties.getString(SPARK_SHUFFLE_SERVICE_ENABED, "true"))
				.set("spark.executor.instances", "16")
		;

		sparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();

		printRuntimeEnv();
	}

	public void printRuntimeEnv()
	{
		logger.info("config = {}", this.configFile);

		logger.info("Spark Driver Runtime Environment...");
		HashMap.HashTrieMap<String, String> map = (HashMap.HashTrieMap<String, String>) sparkSession.conf().getAll();

		Iterator<Tuple2<String, String>> iterator = map.iterator();

		while(iterator.hasNext())
		{
			Tuple2<String, String> tuple = iterator.next();

			System.out.println(String.format("%32s : %s", tuple._1, tuple._2));
		}
	}

	public Dataset<Row> load(String path)
	{
		return sparkSession.read().load(path);
	}

	public boolean isLocal()
	{
		return sparkConf.get("spark.master").contains("local");
	}

	public void save(Dataset<Row> data, String path)
	{
		try {
			if(isLocal())
			{
				FileUtils.deleteDirectory(new File(path));
				data.write().mode(SaveMode.ErrorIfExists).save(path);
			}
			else
			{
				data.write().mode(SaveMode.Overwrite).save(path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void save(Word2VecModel data, String path)
	{
		try {
			data.write().overwrite().save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void save(KMeansModel data, String path)
	{
		try {
			data.write().overwrite().save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getDataPath()
	{
		logger.info("data path : {}", properties.getString(DATA_DIRECTORY));
		return properties.getString(DATA_DIRECTORY, "");
	}

	public String getProperty(String key)
	{
		return properties.getString(key, "");
	}

	public abstract void run(String[] args) throws Exception;

	public abstract void stop() throws Exception;

	public static void main(String[] args) throws Exception
	{
		Word2VecExample word2Vec = new Word2VecExample("config/config.properties");
		word2Vec.stop();
	}
}
