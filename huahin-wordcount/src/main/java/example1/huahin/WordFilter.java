package example1.huahin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.huahinframework.core.Filter;
import org.huahinframework.core.Writer;
import org.huahinframework.core.io.Record;

public class WordFilter extends Filter {

	@Override
	public void filterSetup() {
	}

	@Override
	public void init() {
	}

	@Override
	public void filter(Record record, Writer writer) throws IOException,
			InterruptedException {
		String text = record.getValueString("TEXT");
		StringTokenizer st = new StringTokenizer(text);
		while (st.hasMoreTokens()) {
			String word = st.nextToken();

			Record emitRecord = new Record();
			emitRecord.addGrouping("WORD", word);
			emitRecord.addValue("COUNT", 1);

			writer.write(emitRecord);
		}
	}

}