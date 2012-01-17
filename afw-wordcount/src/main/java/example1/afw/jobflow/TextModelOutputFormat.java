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

import example1.afw.modelgen.dmdl.model.TextModel;

public class TextModelOutputFormat extends
		FileOutputFormat<NullWritable, TextModel> {
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
	public RecordWriter<NullWritable, TextModel> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		return new TextModelRecordWriter(delegate.getRecordWriter(job));
	}

	protected static class TextModelRecordWriter extends
			RecordWriter<NullWritable, TextModel> {
		protected RecordWriter<NullWritable, Text> writer;

		public TextModelRecordWriter(RecordWriter<NullWritable, Text> writer) {
			this.writer = writer;
		}

		@Override
		public void write(NullWritable key, TextModel value)
				throws IOException, InterruptedException {
			if (value != null) {
				writer.write(null, value.getText());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.close(context);
		}
	}
}
