package org.irgroup.spark.ml;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ Word2VecExample.java
 * </pre>
 * <p>
 * 
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 8. 31.
 * @Version : 1.0
 */

public class Word2VecExample extends SparkManager
{

	private static final Logger	logger				= LoggerFactory.getLogger(Word2VecExample.class);

	private String				dataDir;
	private String				rawDataFile			= "terms.txt";
	private String				featureDFFile		= "terms";
	private String				word2vecModelFile	= "word2vec";
	private String				documentVectorFile	= "documentVector";

	List<Row>					document;
	Dataset<Row>				featureDF;
	Dataset<Row>				documentVectorDF;																												        // generated
																																								        // document
	Word2VecModel				model;																																		        // word2vec
																																											        // Model
	/**
	 * @param driverName
	 */
	public Word2VecExample(String dataDir)
	{
		super(Word2VecExample.class.getSimpleName());

		this.dataDir = dataDir;
		rawDataFile = this.dataDir + "/" + rawDataFile;
		featureDFFile = this.dataDir + "/" + featureDFFile;
		word2vecModelFile = this.dataDir + "/" + word2vecModelFile;
		documentVectorFile = this.dataDir + "/" + documentVectorFile;
	}

	private StructType getTermsSchema()
	{
		return new StructType(new StructField[] {
				new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("terms", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
				new StructField("kma", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()),
				new StructField("termskma", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		});
	}

	/**
	 * rawFile format : {id}\n[term-group1]\n[term-group2]
	 *
	 * @param rawFile
	 */
	public void loadRawData(String rawFile)
	{
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(rawFile));
			String line;
			document = new ArrayList<Row>();

			while ((line = in.readLine()) != null)
			{
				String id = line;
				String terms1 = in.readLine();
				String terms2 = in.readLine();

				// The first row is term list
				Row r = RowFactory.create(Integer.parseInt(id),
							Arrays.asList(terms1.split(" ")),
							Arrays.asList(terms2.split(" ")),
							Arrays.asList((terms1 + " " + terms2).split(" ")));

				document.add(r);
			}

			in.close();

			featureDF = sparkSession.createDataFrame(document, getTermsSchema());

		}
		catch (IOException e)
		{
			logger.error(e.getMessage());
		}
	}

	public void fit()
	{
		Word2Vec word2Vec = new Word2Vec()
				// .setInputCol("terms")
				// .setInputCol("kma")
				.setInputCol("termskma")
				.setOutputCol("result")
				.setVectorSize(100)
				.setMinCount(0);
		model = word2Vec.fit(featureDF);
	}

	public void transform()
	{
		documentVectorDF = model.transform(featureDF);
	}

	public void save()
	{
		assert featureDF != null : "Yet not created documentDF.";
		assert model != null : "Yet not created model.";
		assert documentVectorDF != null : "Yet not created documentVectorDF.";

		save(featureDF, featureDFFile);
		save(documentVectorDF, documentVectorFile);
		save(model, word2vecModelFile);
	}

	public void load()
	{
		featureDF = load(featureDFFile).cache();
		model = Word2VecModel.load(word2vecModelFile);
		documentVectorDF = load(documentVectorFile).cache();
	}

	public Dataset<Row> getVectors()
	{
		return model.getVectors().cache();
	}

	public Dataset<Row> getDocumentVectors()
	{
		return documentVectorDF.select("id", "result");
	}

	public Dataset<Row> findSynonym(String word, int count)
	{
		Dataset<Row> synonym = null;

		try {
			synonym = model.findSynonyms(word, count);
		}
		catch (Exception e)
		{
		}

		return synonym;
	}

	public Dataset<Row> getWordVector(String word, boolean like)
	{
		String where = "";

		if(like)
			where = "word like \"%" + word + "%\"";
		else
			where = "word = \"" + word + "\"";

		return getVectors().where(where);
	}

	public Dataset<Row> getDocument(int id)
	{
		return documentVectorDF != null ? documentVectorDF.where("id = " + id) : null;
	}

	public Vector getDocumentVector(int id)
	{
		Dataset<Row> doc = getDocument(id);
		return doc != null ? doc.collectAsList().get(0).getAs("result") : null;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public void run(String[] args) throws Exception
	{
		logger.info("load raw data.");
		loadRawData(rawDataFile);
		logger.info("fit word2vec");
		fit();
		logger.info("transform word2vec");
		transform();
		logger.info("save all");
		save();
	}

	public void stop() throws Exception
	{

	}
}
