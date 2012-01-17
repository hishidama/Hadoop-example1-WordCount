package example1.afw.batch;

import example1.afw.jobflow.WordCountJobFlow2;
import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * バッチクラス。
 */
@Batch(name = "afwWordcountBatch2")
public class WordCountBatch2 extends BatchDescription {

	@Override
	protected void describe() {
		run(WordCountJobFlow2.class).soon();
	}
}
