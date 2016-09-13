package org.irgroup.spark.ml;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ MongoDBManager.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 9.
 * @Version : 1.0
 */

public class MongoDBManager {
	private static final Logger logger = LoggerFactory.getLogger(MongoDBManager.class);

	private JavaSparkContext context;
	private SparkConf config;
	private SparkSession session;

	public MongoDBManager(String master) {
		config = new SparkConf()
				.setAppName(MongoDBManager.class.getSimpleName())
				.setMaster(master);
	}

	public void connect() {
		context = new JavaSparkContext(config);
		session = SparkSession.builder()
				.config(config)
				.getOrCreate();
	}

	public void disconnect() {
		context.sc().stop();
	}

	public void setConfig(String key, String value) {
		config.set(key, value);
	}

	public ReadConfig setReadConfig(String key, String value) {
		// ReadConfig.withOption을 사용하려면 SparkConf에 Key값이 존재해야 함(버그인 듯)
		// 그래서 config에 직접 set 해 주고 withOption을 사용하지 않는다
		config.set(key, value);
		return ReadConfig.create(config);
	}

	public Dataset<Row> select(String collection, String[] fields) {
		List<Column> list = new ArrayList<Column>();
		for (String field : fields) {
			list.add(new Column(field));
		}

		Seq<Column> seq = scala.collection.JavaConversions.asScalaBuffer(list).toSeq();

		return MongoSpark.load(context, setReadConfig("spark.mongodb.input.collection", collection))
				.toDF()
				.select(seq);
	}

	public Dataset<Row> select(String collection, String sql) {
		Dataset<Row> df = MongoSpark.load(context, setReadConfig("spark.mongodb.input.collection", collection))
				.toDF()
				.persist(StorageLevel.MEMORY_AND_DISK());

		df.registerTempTable(collection);
		return session.sqlContext().sql(sql).cache();
	}

	public void save(Dataset<Row> data, String path) {
		try {
			FileUtils.deleteDirectory(new File(path));
			data.write().mode(SaveMode.ErrorIfExists).save(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void saveAsText(Dataset<Row> data, String path) {
		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new FileWriter(new File(path)));

			StructField[] fields = data.schema().fields();
			Iterator<Row> itr = data.toLocalIterator();

			int count = 0;
			while (itr.hasNext()) {
				count++;
				Row r = itr.next();

				for (int i = 0; i < fields.length; i++) {
					if (fields[i].dataType().typeName().equals("array")) {
						WrappedArray<Object> e = r.getAs(i);
						writer.write(e.mkString(" "));
					}
					else {
						writer.write("" + r.getAs(i));
					}
					writer.newLine();
				}
				if (count % 1000 == 0)
					logger.info(count + "th complted.");
			}

			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public JavaSparkContext getContext() {
		return context;
	}

}
