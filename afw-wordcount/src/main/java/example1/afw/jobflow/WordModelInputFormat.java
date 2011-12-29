package example1.afw.jobflow;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import example1.afw.modelgen.dmdl.model.WordModel;

public class WordModelInputFormat extends
		FileInputFormat<NullWritable, WordModel> {

	@Override
	public RecordReader<NullWritable, WordModel> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WordModelRecordReader();
	}

	protected static class WordModelRecordReader extends
			RecordReader<NullWritable, WordModel> {
		protected LineRecordReader reader = new LineRecordReader();
		protected WordModel model = new WordModel();
		protected StringTokenizer tokenizer = null;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (tokenizer != null && tokenizer.hasMoreTokens()) {
				return true;
			}
			return reader.nextKeyValue();
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			if (tokenizer != null && tokenizer.hasMoreTokens()) {
				return NullWritable.get();
			}
			if (reader.getCurrentKey() != null) {
				return NullWritable.get();
			} else {
				return null;
			}
		}

		@Override
		public WordModel getCurrentValue() throws IOException,
				InterruptedException {
			for (;;) {
				if (tokenizer == null) {
					Text text = reader.getCurrentValue();
					if (text != null) {
						tokenizer = new StringTokenizer(text.toString());
					} else {
						return null;
					}
				}
				if (tokenizer.hasMoreTokens()) {
					String word = tokenizer.nextToken();
					model.setWordAsString(word);
					return model;
				} else {
					tokenizer = null;
				}
			}
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
}
