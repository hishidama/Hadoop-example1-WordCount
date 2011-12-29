package example1.afw.operator;

import com.asakusafw.vocabulary.operator.Summarize;

import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.modelgen.dmdl.model.WordModel;

/**
 * WordCountの演算子。
 */
public abstract class WordCountOperator {

	@Summarize
	public abstract WordCountModel count(WordModel wm);
}
