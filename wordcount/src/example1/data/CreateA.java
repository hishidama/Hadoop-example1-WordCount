package example1.data;

import org.apache.hadoop.util.ToolRunner;

public class CreateA extends Create {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new CreateA(), args);
		System.exit(r);
	}

	static final String[] SS = { "Hello World", "Hello Hadoop", "WordCount" };

	protected String data(int i) {
		return SS[i % SS.length];
	}
}
