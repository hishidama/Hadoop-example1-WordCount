package example1.afw.jobflow;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

import example1.afw.modelgen.dmdl.model.TextModel;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.operator.WordCountOperatorFactory;
import example1.afw.operator.WordCountOperatorFactory.Count;
import example1.afw.operator.WordCountOperatorFactory.Split;

/**
 * ジョブフロークラス
 */
@JobFlow(name = "wordcount")
public class WordCountJobFlow extends FlowDescription {

	private In<TextModel> in;
	private Out<WordCountModel> out;

	public WordCountJobFlow(
			@Import(name = "in", description = TextFromCsv.class) In<TextModel> in,
			@Export(name = "out", description = WordCountToCsv.class) Out<WordCountModel> out) {
		this.in = in;
		this.out = out;
	}

	@Override
	public void describe() {
		WordCountOperatorFactory operators = new WordCountOperatorFactory();

		Split split = operators.split(in);
		Count count = operators.count(split.out);

		out.add(count.out);
	}
}
