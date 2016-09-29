package org.irgroup.spark.ml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Scanner;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ ExampleCmd.java
 * </pre>
 * <p>
 * 
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 5.
 * @Version : 1.0
 */

public class ExampleCmd
{
	static
	{
		System.setProperty("logback.configurationFile", "src/main/resources/logback.xml");
	}
	private static final Logger logger = LoggerFactory.getLogger(ExampleCmd.class);

	private enum MainCommand
	{
		exit, statistics, info, synonym, similarity, predict
	}

	private enum StatisticsCommand
	{
		exit, word, word2vec, document, cluster
	}

	private enum InfoCommand
	{
		exit, word, document, cluster
	}

	private enum SimilarityCommand
	{
		exit, word2word, word2doc, word2cluster, doc2word, doc2doc, doc2cluster, cluster2word, cluster2doc, cluster2cluster
	}

	private Scanner				scan	= new Scanner(System.in);
	private ExampleCmdLauncher	launcher;

	public ExampleCmd(String dataDir)
	{
		launcher = new ExampleCmdLauncher(dataDir);
	}

	private <E extends Enum<E>> void printCommand(Class<E> e, String command)
	{
		System.out.println(String.format("\ncommand list (%s) :", command));

		for (E entry : EnumSet.allOf(e))
		{
			System.out.println("\t" + entry.ordinal() + ". " + entry.name());
		}

		System.out.print(String.format("\n$%s > ", command));
	}

	private void statisticsCommandPrompt()
	{
		while (true)
		{
			printCommand(StatisticsCommand.class, MainCommand.statistics.name());

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase(StatisticsCommand.exit.name()) == 0
					|| cmd[0].compareTo("" + StatisticsCommand.exit.ordinal()) == 0)
				break;

			if (cmd[0].compareToIgnoreCase(StatisticsCommand.word.name()) == 0
					|| cmd[0].compareTo("" + StatisticsCommand.word.ordinal()) == 0)
			{
				launcher.statisticsWord();
			}
			else if (cmd[0].compareToIgnoreCase(StatisticsCommand.word2vec.name()) == 0
					|| cmd[0].compareTo("" + StatisticsCommand.word2vec.ordinal()) == 0)
			{
				launcher.statisticsWord2Vec();
			}
			else if (cmd[0].compareToIgnoreCase(StatisticsCommand.document.name()) == 0
					|| cmd[0].compareTo("" + StatisticsCommand.document.ordinal()) == 0)
			{
				launcher.statisticsDocument();
			}
			else if (cmd[0].compareToIgnoreCase(StatisticsCommand.cluster.name()) == 0
					|| cmd[0].compareTo("" + StatisticsCommand.cluster.ordinal()) == 0)
			{
				launcher.statisticsCluster();
			}
		}
	}

	private void infoCommandPrompt()
	{
		while (true)
		{
			printCommand(InfoCommand.class, MainCommand.info.name());

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase(InfoCommand.exit.name()) == 0 || cmd[0].compareTo("" + InfoCommand.exit.ordinal()) == 0)
				break;

			if (cmd[0].compareToIgnoreCase(InfoCommand.word.name()) == 0 || cmd[0].compareTo("" + InfoCommand.word.ordinal()) == 0)
			{
				System.out.println(
						String.format("\"%s %s\" usage : [keyword] [number]", MainCommand.info.name(), InfoCommand.word.name()));
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 2)
				{
					launcher.printWordVector(param[0], Integer.parseInt(param[1]));
				}
			}
			else if (cmd[0].compareToIgnoreCase(InfoCommand.document.name()) == 0
					|| cmd[0].compareTo("" + InfoCommand.document.ordinal()) == 0)
			{
				System.out
						.println(String.format("\"%s %s\" usage : [doc-id]", MainCommand.info.name(), InfoCommand.document.name()));
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 1)
				{
					launcher.printDocumentVector(Integer.parseInt(param[0]));
				}
			}
			else if (cmd[0].compareToIgnoreCase(InfoCommand.cluster.name()) == 0
					|| cmd[0].compareTo("" + InfoCommand.cluster.ordinal()) == 0)
			{
				System.out.println(
						String.format("\"%s %s\" usage : [cluster-id]", MainCommand.info.name(), InfoCommand.cluster.name()));
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 1)
				{
					launcher.printClusterVector(Integer.parseInt(param[0]));
				}
			}
		}
	}

	private void synonymCommandPrompt()
	{
		while (true)
		{
			System.out.println(String.format("\n%s usage : [keyword] [number]", MainCommand.synonym.name()));
			System.out.print("? ");
			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase("exit") == 0)
				break;
			else if (cmd.length >= 2)
			{
				launcher.findSynonym(cmd[0], Integer.parseInt(cmd[1]));
			}
		}
	}

	private void similarityCommandPrompt()
	{
		while (true)
		{
			System.out.println(
					String.format("\n%s usage : command [word|doc-id|cluster-id] [number]", MainCommand.similarity.name()));
			printCommand(SimilarityCommand.class, MainCommand.similarity.name());

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase(SimilarityCommand.exit.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.exit.ordinal()) == 0)
				break;

			if (cmd.length < 3)
				continue;

			if (cmd[0].compareToIgnoreCase(SimilarityCommand.word2word.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.word2word.ordinal()) == 0)
			{
				launcher.printWord2WordSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.word2doc.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.word2doc.ordinal()) == 0)
			{
				launcher.printWord2DocSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.word2cluster.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.word2cluster.ordinal()) == 0)
			{
				launcher.printWord2ClusterSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.doc2word.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.doc2word.ordinal()) == 0)
			{
				launcher.printDoc2WordSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.doc2doc.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.doc2doc.ordinal()) == 0)
			{
				launcher.printDoc2DocSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.doc2cluster.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.doc2cluster.ordinal()) == 0)
			{
				launcher.printDoc2ClusterSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.cluster2word.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.cluster2word.ordinal()) == 0)
			{
				launcher.printCluster2WordSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.cluster2doc.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.cluster2doc.ordinal()) == 0)
			{
				launcher.printCluster2DocSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase(SimilarityCommand.cluster2cluster.name()) == 0
					|| cmd[0].compareTo("" + SimilarityCommand.cluster2cluster.ordinal()) == 0)
			{
				launcher.printCluster2ClusterSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}

		}
	}

	private void predictCommandPrompt()
	{
		while (true)
		{
			System.out.println(String.format("\n%s usage : [condition] [relation] [question] [count]", MainCommand.predict.name()));
			System.out.print("? ");
			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase("exit") == 0)
				break;
			else if (cmd.length >= 4)
			{
				launcher.predict(cmd[0], cmd[1], cmd[2], Integer.parseInt(cmd[3]));
			}
		}
	}

	private void commandPrompt()
	{

		while (true)
		{
			printCommand(MainCommand.class, "");

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase(MainCommand.exit.name()) == 0 || cmd[0].compareToIgnoreCase("quit") == 0
					|| cmd[0].compareTo("" + MainCommand.exit.ordinal()) == 0)
			{
				try
				{
					launcher.word2Vec.stop();
					launcher.kmean.stop();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				break;
			}
			else if (cmd[0].compareToIgnoreCase(MainCommand.statistics.name()) == 0
					|| cmd[0].compareTo("" + MainCommand.statistics.ordinal()) == 0)
			{
				statisticsCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase(MainCommand.synonym.name()) == 0
					|| cmd[0].compareTo("" + MainCommand.synonym.ordinal()) == 0)
			{
				synonymCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase(MainCommand.info.name()) == 0
					|| cmd[0].compareTo("" + MainCommand.info.ordinal()) == 0)
			{
				infoCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase(MainCommand.similarity.name()) == 0
					|| cmd[0].compareTo("" + MainCommand.similarity.ordinal()) == 0)
			{
				similarityCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase(MainCommand.predict.name()) == 0
					|| cmd[0].compareTo("" + MainCommand.predict.ordinal()) == 0)
			{
				predictCommandPrompt();
			}
		}

	}

	public static void main(String[] args) throws Exception
	{

		if (args.length < 1)
		{
			System.out.println("Usage : " + ExampleCmd.class.getName() + " [data-path]");
			System.exit(0);
		}

		ExampleCmd command = new ExampleCmd(args[0]);

		command.commandPrompt();
	}
}
