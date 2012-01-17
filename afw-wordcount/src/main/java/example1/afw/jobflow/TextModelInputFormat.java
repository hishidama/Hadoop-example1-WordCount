package example1.afw.jobflow;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import example1.afw.modelgen.dmdl.model.TextModel;

public class TextModelInputFormat extends
		FileInputFormat<NullWritable, TextModel> {

	@Override
	public RecordReader<NullWritable, TextModel> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new TextModelRecordReader();
	}

	protected static class TextModelRecordReader extends
			RecordReader<NullWritable, TextModel> {
		protected LineRecordReader reader = new LineRecordReader();
		protected TextModel model = new TextModel();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			if (reader.getCurrentKey() != null) {
				return NullWritable.get();
			} else {
				return null;
			}
		}

		@Override
		public TextModel getCurrentValue() throws IOException,
				InterruptedException {
			Text text = reader.getCurrentValue();
			if (text != null) {
				model.setText(text);
				return model;
			} else {
				return null;
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
