package example.flow;

import example.entity.TextEntity;
import example.entity.WordCountEntity;
import java.util.StringTokenizer;
import jp.co.cac.azarea.cluster.Main;
import jp.co.cac.azarea.cluster.planner.job.EntityFlow;
import jp.co.cac.azarea.cluster.planner.job.SimpleEntityFlowManager;
import jp.co.cac.azarea.cluster.planner.operation.Conversion;
import jp.co.cac.azarea.cluster.planner.operation.EntityFile;
import jp.co.cac.azarea.cluster.planner.operation.Group;
import jp.co.cac.azarea.cluster.util.Generated;

@Generated("AZAREA-Cluster 1.0")
public class WordCountFlow extends EntityFlow {

	@Override
	protected void initialize() {
		EntityFile<TextEntity> textEntity = getInput(TextEntity.class);
		Conversion<TextEntity, WordCountEntity> splitEntity = new Conversion<TextEntity, WordCountEntity>(textEntity) {
			@Override
			protected void convert(TextEntity entity) {
				StringTokenizer tokenizer = new StringTokenizer(entity.text);
				while (tokenizer.hasMoreTokens()) {
					WordCountEntity result = new WordCountEntity();
					result.word = tokenizer.nextToken();
					result.count = 1;
					output(result);
				}
			}
		};
		Group<WordCountEntity> coountEntity = new Group<WordCountEntity>(splitEntity, "word") {
			@Override
			protected void doSummarize(WordCountEntity summary,
					WordCountEntity another) {
				summary.count += another.count;
			}
		};
		setOutput(coountEntity);
	}
	public static void main(String[] args) throws Exception {
		Main.execute(SimpleEntityFlowManager.class.getName(),
				WordCountFlow.class.getName(), args);
	}

}
