package example.flow;

import java.io.IOException;

import jp.co.cac.azarea.cluster.planner.job.SimpleEntityFlowManager;
import jp.co.cac.azarea.cluster.tester.MapReduceJobManagerTester;
import jp.co.cac.azarea.cluster.util.Generated;

@Generated("AZAREA-Cluster 1.0")
public class WordCountFlowTest {

	public static void main(String[] args) throws IOException {
		MapReduceJobManagerTester tester = new MapReduceJobManagerTester(
				new SimpleEntityFlowManager());
		tester.test("../data", WordCountFlow.class.getName());
	}
}