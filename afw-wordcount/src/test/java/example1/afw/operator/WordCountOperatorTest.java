package example1.afw.operator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.asakusafw.runtime.testing.MockResult;

import example1.afw.modelgen.dmdl.model.TextModel;
import example1.afw.modelgen.dmdl.model.WordModel;

public class WordCountOperatorTest {

	@Test
	public void testSplit() {
		WordCountOperator operator = new WordCountOperatorImpl();
		TextModel tm = new TextModel();

		MockResult<WordModel> out = new MockResult<WordModel>() {
			@Override
			protected WordModel bless(WordModel result) {
				WordModel clone = new WordModel();
				clone.copyFrom(result);
				return clone;
			}
		};

		tm.setTextAsString("Hello World");
		operator.split(tm, out);
		tm.setTextAsString("Asakusa");
		operator.split(tm, out);

		List<WordModel> r = out.getResults();
		assertThat(r.size(), is(3));
		assertThat(r.get(0).getWordAsString(), is("Hello"));
		assertThat(r.get(1).getWordAsString(), is("World"));
		assertThat(r.get(2).getWordAsString(), is("Asakusa"));
	}
}
