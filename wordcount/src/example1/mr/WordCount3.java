package example1.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

/**
 * WordCount HashMap（適宜リセットする）版
 */
public class WordCount3 {
	public static final String HASH_SIZE_KEY = "HASH_SIZE";

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		int hashSize;
		CounterMap counterMap;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			hashSize = context.getConfiguration().getInt(HASH_SIZE_KEY, 256);
			counterMap = new CounterMap(hashSize);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				counterMap.add(word, 1);
			}

			if (counterMap.size() > hashSize) {
				write(context);
				counterMap.clear();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			write(context);

			super.cleanup(context);
		}

		private Text word = new Text();
		private IntWritable count = new IntWritable();

		void write(Context context) throws IOException, InterruptedException {
			for (Counter c : counterMap.values()) {
				word.set(c.key);
				count.set(c.count);
				context.write(word, count);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount-java3");
		job.setJarByClass(WordCount3.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().set(HASH_SIZE_KEY, args[2]);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
