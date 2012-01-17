package example1.afw.jobflow;

import org.junit.Test;

import com.asakusafw.testdriver.JobFlowTester;

import example1.afw.modelgen.dmdl.model.TextModel;
import example1.afw.modelgen.dmdl.model.WordCountModel;

/**
 * ジョブフローのテストクラス
 */
public class WordCountJobFlow2Test {

	@Test
	public void testExample() {
		JobFlowTester driver = new JobFlowTester(getClass());

		driver.input("in", TextModel.class).prepare("text_model.xls#input");
		driver.output("out", WordCountModel.class).verify(
				"word_count_model.xls#output", "word_count_model.xls#rule");

		driver.runTest(WordCountJobFlow2.class);
	}
}
