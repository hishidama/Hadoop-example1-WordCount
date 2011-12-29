package example1.afw.jobflow;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

import example1.afw.modelgen.dmdl.model.WordModel;
import example1.afw.modelgen.dmdl.model.WordCountModel;
import example1.afw.operator.WordCountOperatorFactory;
import example1.afw.operator.WordCountOperatorFactory.Count;

/**
 * ジョブフロークラス
 */
@JobFlow(name = "wordcount")
public class WordCountJobFlow extends FlowDescription {

	private In<WordModel> in;
	private Out<WordCountModel> out;

	public WordCountJobFlow(
			@Import(name = "in", description = WordImporter.class) In<WordModel> in,
			@Export(name = "out", description = WordCountExporter.class) Out<WordCountModel> out) {
		this.in = in;
		this.out = out;
	}

	@Override
	public void describe() {
		WordCountOperatorFactory operators = new WordCountOperatorFactory();

		Count count = operators.count(in);

		out.add(count.out);
	}
}
