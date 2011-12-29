package example1.cascading;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class WordCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new WordCount(), args);
		System.exit(r);
	}

	// フィールド名の定義
	public static final String F_LINE = "line";
	public static final String F_WORD = "word";
	public static final String F_COUNT = "count";

	@Override
	public int run(String[] args) throws Exception {
		// 入出力ディレクトリーの指定
		Tap source = new Hfs(new TextLine(new Fields(F_LINE)),
				makeQualifiedPath(args[0]));
		Tap sink = new Hfs(new TextDelimited(new Fields(F_WORD, F_COUNT),
				false, "\t"), makeQualifiedPath(args[1]), SinkMode.REPLACE);

		// Pipeの初期化
		Pipe pipe = new Pipe("wordcount-pipe");

		// 行を単語に分割する。
		pipe = new Each(pipe, new RegexSplitGenerator(new Fields(F_WORD),
				"[ \t\n\r\f]+"));

		// 単語毎にカウントする。
		// pipe = new GroupBy(pipe, new Fields(F_WORD));
		// pipe = new Every(pipe, new Count(new Fields(F_COUNT)));
		pipe = new CountBy(pipe, new Fields(F_WORD), new Fields(F_COUNT));

		// 実行
		FlowConnector flowConnector = new FlowConnector();
		Flow flow = flowConnector.connect("wordcount-cascading", source, sink,
				pipe);
		flow.complete();

		return 0;
	}

	public String makeQualifiedPath(String path) throws IOException {
		FileSystem fs = FileSystem.get(super.getConf());
		return new Path(path).makeQualified(fs).toString();
	}
}
