package example1.afw.operator;

import java.util.StringTokenizer;

import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Summarize;

import example1.afw.modelgen.dmdl.model.TextModel;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.modelgen.dmdl.model.WordModel;

/**
 * WordCountの演算子。
 */
public abstract class WordCountOperator {

	WordModel wm = new WordModel();

	@Extract
	public void split(TextModel tm, Result<WordModel> out) {

		StringTokenizer tokenizer = new StringTokenizer(tm.getTextAsString());
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			wm.setWordAsString(token);

			out.add(wm);
		}
	}

	@Summarize(partialAggregation = PartialAggregation.PARTIAL)
	public abstract WordCountModel count(WordModel wm);
}
