package example1.huahin;

import java.io.IOException;

import org.huahinframework.core.Summarizer;
import org.huahinframework.core.Writer;
import org.huahinframework.core.io.Record;

public class WordSummarizer extends Summarizer {

	@Override
	public void summarizerSetup() {
	}

	@Override
	public void init() {
	}

	@Override
	public void summarizer(Writer writer) throws IOException,
			InterruptedException {
		Record groupRecord = super.getGroupingRecord();
		String word = groupRecord.getGroupingString("WORD");

		int count = 0;
		while (super.hasNext()) {
			Record record = super.next(writer);
			count += record.getValueInteger("COUNT");
		}

		Record emitRecord = new Record();
		emitRecord.addGrouping("WORD", word);
		emitRecord.addValue("COUNT", count);
		writer.write(emitRecord);
	}
}
