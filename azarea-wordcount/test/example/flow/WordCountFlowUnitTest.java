package example.flow;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import jp.co.cac.azarea.cluster.planner.job.SimpleEntityFlowManager;
import jp.co.cac.azarea.cluster.tester.MapReduceJobManagerTester;

public class WordCountFlowUnitTest {

	@Test
	public void wordCountFlow() throws IOException {
		MapReduceJobManagerTester tester = new MapReduceJobManagerTester(
				new SimpleEntityFlowManager());
		assertTrue(tester.testAndAssert("../data", "../expected",
				WordCountFlow.class.getName()));
	}
}