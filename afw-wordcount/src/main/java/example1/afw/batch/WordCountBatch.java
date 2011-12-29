package example1.afw.batch;

import example1.afw.jobflow.WordCountJobFlow;
import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * バッチクラス。
 */
@Batch(name = "afwWordcountBatch")
public class WordCountBatch extends BatchDescription {

	@Override
	protected void describe() {
		run(WordCountJobFlow.class).soon();
	}
}
