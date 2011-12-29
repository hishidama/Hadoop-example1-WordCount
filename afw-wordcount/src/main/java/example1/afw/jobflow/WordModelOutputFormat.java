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

import example1.afw.modelgen.dmdl.model.WordModel;

public class WordModelOutputFormat extends
		FileOutputFormat<NullWritable, WordModel> {
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
	public RecordWriter<NullWritable, WordModel> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		return new WordModelRecordWriter(delegate.getRecordWriter(job));
	}

	protected static class WordModelRecordWriter extends
			RecordWriter<NullWritable, WordModel> {
		protected RecordWriter<NullWritable, Text> writer;

		public WordModelRecordWriter(RecordWriter<NullWritable, Text> writer) {
			this.writer = writer;
		}

		@Override
		public void write(NullWritable key, WordModel value)
				throws IOException, InterruptedException {
			if (value != null) {
				writer.write(null, value.getWord());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.close(context);
		}
	}
}
