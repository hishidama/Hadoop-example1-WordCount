package example1.data;

import org.apache.hadoop.util.ToolRunner;

public class CreateB extends Create {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new CreateB(), args);
		System.exit(r);
	}

	protected String data(int i) {
		return Integer.toString(i);
	}
}
