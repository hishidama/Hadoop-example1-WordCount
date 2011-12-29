package example1.afw.jobflow;

import org.junit.Test;

import com.asakusafw.testdriver.JobFlowTester;

import example1.afw.modelgen.dmdl.model.WordModel;
import example1.afw.modelgen.dmdl.model.WordCountModel;

/**
 * ジョブフローのテストクラス
 */
public class WordCountJobFlowTest {

	@Test
	public void testExample() {
		JobFlowTester driver = new JobFlowTester(getClass());

		driver.input("in", WordModel.class).prepare("word_model.xls#input");
		driver.output("out", WordCountModel.class).verify(
				"word_count_model.xls#output", "word_count_model.xls#rule");

		driver.runTest(WordCountJobFlow.class);
	}
}
