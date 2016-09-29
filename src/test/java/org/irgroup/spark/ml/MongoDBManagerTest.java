package org.irgroup.spark.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ MongoDBManagerTest.java
 * </pre>
 * <p>
 * 
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 12.
 * @Version : 1.0
 */

public class MongoDBManagerTest
{
	private MongoDBManager	mongodb;
	private String			uri			= "mongodb://praha:praha!%40#@praha-manage-stage/praha";
	private String			database	= "praha";
	private String			dataDir		= "data";

	@Before
	public void setUp() throws Exception
	{
		System.out.println("start mongodb session");

		mongodb = new MongoDBManager("local");
		mongodb.setConfig("spark.mongodb.input.uri", uri);
		mongodb.setConfig("spark.mongodb.input.database", database);
		mongodb.setConfig("spark.mongodb.output.uri", uri);
		mongodb.setConfig("spark.mongodb.output.database", database);

		mongodb.connect();
	}

	@After
	public void tearDown() throws Exception
	{
		mongodb.disconnect();
	}

	@Test
	public void schema() throws Exception
	{
		Dataset<Row> df = mongodb.select("dealFeatureInfosWithIdx",
				"select did, features.dc[0] as dc, features.dckma[0] as dckma from dealFeatureInfosWithIdx");
		df.show(false);

		StructType schema = df.schema();
		schema.printTreeString();

		StructField[] sfs = schema.fields();

		System.out.println("StructType length : " + schema.length());
		System.out.println("StructField length : " + sfs.length);

		int i = 0;
		for (StructField sf : sfs)
		{
			System.out.println();
			System.out.println(sf.toString());
			System.out.println("sf name = " + sf.name());
			System.out.println("data type json = " + sf.dataType().prettyJson());
			System.out.println("catalog string = " + sf.dataType().catalogString());
			System.out.println("type name      = " + sf.dataType().typeName());
			System.out.println("simple string  = " + sf.dataType().simpleString());
			System.out.println("default size   = " + sf.dataType().defaultSize());
			System.out.println("sql            = " + sf.dataType().sql());

			if (sf.dataType() == DataTypes.IntegerType)
			{
				System.out.println(i + "th IntegerType.");
			}
			else if (sf.dataType().typeName().equals("array"))
			{
				System.out.println(i + "th ArrayType.");
			}
			else if (sf.dataType().typeName().equals("struct"))
			{
				System.out.println(i + "th StructType.");
			}
			i++;
		}
	}

	@Test
	public void save() throws Exception
	{
		System.out.println("select");
		// Dataset<Row> df = mongodb.select("dealFeatureInfosWithIdx", "select * from dealFeatureInfosWithIdx");
		// df.show(false);
		// df.printSchema();
		// System.out.println("save");
		// mongodb.save(df, "data/mongo");

		Dataset<Row> ndf = mongodb.select("dealFeatureInfosWithIdx",
				"select did, features.dc[0] as dc, features.dckma[0] as dckma from dealFeatureInfosWithIdx");
		ndf.show(false);
		ndf.printSchema();

		System.out.println("saveAsText");
		mongodb.saveAsText(ndf, dataDir + "/mongo.txt");
	}

}