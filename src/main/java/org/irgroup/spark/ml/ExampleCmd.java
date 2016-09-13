package org.irgroup.spark.ml;

import java.util.Scanner;

/**
 * <pre>
 *      org.irgroup.spark.ml
 *        |_ ExampleCmd.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 5.
 * @Version : 1.0
 * @TODO info 명령을 document(원본, 벡터 클러스터링 결과까지), word2vec, cluster로 나누어 정리할 것
 * @TODO mongoDB 데이터로 raw data 생성하는 로직 구현할 것
 */

public class ExampleCmd {

	private enum MainCommand {exit, statistics, info, synonym, similarity}

	private enum PrintCommand {exit, word, document, cluster}

	private enum SimilarityCommand {exit, word2word, word2doc, word2cluster, doc2word, doc2doc, doc2cluster, cluster2word, cluster2doc, cluster2cluster}

	private Scanner scan = new Scanner(System.in);
	private ExampleCmdLauncher launcher = new ExampleCmdLauncher();

	public ExampleCmd() {
	}

	private void infoCommandPrompt() {
		while (true) {
			System.out.println("\ncommand list (info) :");
			for (PrintCommand mc : PrintCommand.values()) {
				System.out.println("\t" + mc.name());
			}
			System.out.print("$\"print\" cmd > ");

			String[] subcmd = scan.nextLine().split(" ");

			if (subcmd[0].compareToIgnoreCase("exit") == 0)
				break;

			if (subcmd[0].compareToIgnoreCase("word") == 0) {
				System.out.println("\"print word\" usage : [keyword] [number]");
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 2) {
					launcher.printWordVector(param[0], Integer.parseInt(param[1]));
				}
			}
			else if (subcmd[0].compareToIgnoreCase("document") == 0) {
				System.out.println("\"print document\" usage : [doc-id]");
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 1) {
					launcher.printDocumentVector(Integer.parseInt(param[0]));
				}
			}
			else if (subcmd[0].compareToIgnoreCase("cluster") == 0) {
				System.out.println("\"print cluster\" usage : [cluster-id]");
				System.out.print("? ");

				String[] param = scan.nextLine().split(" ");
				if (param.length >= 1) {
					launcher.printClusterVector(Integer.parseInt(param[0]));
				}
			}
		}
	}

	private void synonymCommandPrompt() {
		while (true) {
			System.out.println("\nsynonym usage : [keyword] [number]");
			System.out.print("? ");
			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase("exit") == 0)
				break;
			else if (cmd.length >= 2) {
				launcher.findSynonym(cmd[0], Integer.parseInt(cmd[1]));
			}
		}
	}

	private void similarityCommandPrompt() {
		while (true) {
			System.out.println("\nsimilarity usage : command [word|doc-id|cluster-id] [number]");
			System.out.println("command list (similarity) :");
			for (SimilarityCommand mc : SimilarityCommand.values()) {
				System.out.println("\t" + mc.name());
			}
			System.out.print("$\"similarity\" cmd > ");

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase("exit") == 0)
				break;

			if (cmd.length < 3)
				continue;

			if (cmd[0].compareToIgnoreCase("word2word") == 0) {
				launcher.printWord2WordSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("word2doc") == 0) {
				launcher.printWord2DocSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("word2cluster") == 0) {
				launcher.printWord2ClusterSimilarity(cmd[1], Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("doc2word") == 0) {
				launcher.printDoc2WordSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("doc2doc") == 0) {
				launcher.printDoc2DocSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("doc2cluster") == 0) {
				launcher.printDoc2ClusterSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("cluster2word") == 0) {
				launcher.printCluster2WordSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("cluster2doc") == 0) {
				launcher.printCluster2DocSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}
			else if (cmd[0].compareToIgnoreCase("cluster2cluster") == 0) {
				launcher.printCluster2ClusterSimilarity(Integer.parseInt(cmd[1]), Integer.parseInt(cmd[2]));
			}

		}
	}

	private void commandPrompt() {

		while (true) {
			System.out.println("\ncommand list :");
			for (MainCommand mc : MainCommand.values()) {
				System.out.println("\t" + mc.name());
			}
			System.out.print("$cmd > ");

			String[] cmd = scan.nextLine().split(" ");

			if (cmd[0].compareToIgnoreCase("exit") == 0 || cmd[0].compareToIgnoreCase("quit") == 0) {
				break;
			}
			else if (cmd[0].compareToIgnoreCase("statistics") == 0) {
				launcher.statistics();
			}
			else if (cmd[0].compareToIgnoreCase("synonym") == 0) {
				synonymCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase("info") == 0) {
				infoCommandPrompt();
			}
			else if (cmd[0].compareToIgnoreCase("similarity") == 0) {
				similarityCommandPrompt();
			}
		}

	}

	public static void main(String[] args) throws Exception {

		ExampleCmd command = new ExampleCmd();

		command.commandPrompt();
	}
}
