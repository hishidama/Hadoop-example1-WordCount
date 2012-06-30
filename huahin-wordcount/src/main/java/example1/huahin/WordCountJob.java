package example1.huahin;

import org.huahinframework.core.Runner;

public class WordCountJob {
	public static void main(String[] args) {
		Runner runner = new Runner();
		runner.addJob("wordcount-huahin", WordCountTool.class);

		int status = runner.run("wordcount-huahin", args);
		System.exit(status);
	}
}
