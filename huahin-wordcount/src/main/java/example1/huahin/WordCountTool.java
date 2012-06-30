package example1.huahin;

import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.SimpleJobTool;

public class WordCountTool extends SimpleJobTool {

	@Override
	protected String setInputPath(String[] args) {
		return args[0];
	}

	@Override
	protected String setOutputPath(String[] args) {
		return args[1];
	}

	@Override
	protected void setup() throws Exception {
		final String[] labels = new String[] { "TEXT" };

		SimpleJob job1 = addJob(labels, "\n");
		job1.setFilter(WordFilter.class);
		job1.setSummaizer(WordSummarizer.class);
	}
}
