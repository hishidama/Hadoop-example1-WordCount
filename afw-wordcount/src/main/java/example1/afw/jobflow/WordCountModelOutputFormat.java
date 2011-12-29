package example1.afw.jobflow;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import example1.afw.modelgen.dmdl.model.WordCountModel;

public class WordCountModelOutputFormat extends
		FileOutputFormat<NullWritable, WordCountModel> {
	protected TextOutputFormat<NullWritable, Text> delegate = new TextOutputFormat<NullWritable, Text>();

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		delegate.checkOutputSpecs(job);
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		return delegate.getDefaultWorkFile(context, extension);
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext context) throws IOException {
		return delegate.getOutputCommitter(context);
	}

	@Override
	public RecordWriter<NullWritable, WordCountModel> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		return new WordCountModelRecordWriter(delegate.getRecordWriter(job));
	}

	protected static class WordCountModelRecordWriter extends
			RecordWriter<NullWritable, WordCountModel> {
		protected RecordWriter<NullWritable, Text> writer;

		public WordCountModelRecordWriter(
				RecordWriter<NullWritable, Text> writer) {
			this.writer = writer;
		}

		private Text text = new Text();

		@Override
		public void write(NullWritable key, WordCountModel value)
				throws IOException, InterruptedException {
			if (value != null) {
				String s = value.getWordAsString() + "\t" + value.getCount();
				text.set(s);
				writer.write(null, text);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.close(context);
		}
	}
}
