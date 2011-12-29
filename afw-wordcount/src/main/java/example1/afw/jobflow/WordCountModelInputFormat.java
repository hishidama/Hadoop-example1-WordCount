package example1.afw.jobflow;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import example1.afw.modelgen.dmdl.model.WordCountModel;

public class WordCountModelInputFormat extends
		FileInputFormat<NullWritable, WordCountModel> {

	@Override
	public RecordReader<NullWritable, WordCountModel> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WordCountModelRecordReader();
	}

	protected static class WordCountModelRecordReader extends
			RecordReader<NullWritable, WordCountModel> {
		protected LineRecordReader reader = new LineRecordReader();
		protected WordCountModel model = new WordCountModel();

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
		public WordCountModel getCurrentValue() throws IOException,
				InterruptedException {
			Text text = reader.getCurrentValue();
			if (text != null) {
				String[] ss = text.toString().split("\t");
				model.setWordAsString(ss[0]);
				model.setCount(Long.parseLong(ss[1]));
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
