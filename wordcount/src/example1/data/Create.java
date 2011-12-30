package example1.data;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public abstract class Create extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		int size = Integer.parseInt(args[0]);
		size *= 1024 * 1024; // MB
		Path f = new Path(args[1]);
		System.out.println("create " + f);

		FileSystem fs = FileSystem.get(getConf());
		FSDataOutputStream os = fs.create(f);
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,
					"UTF-8"));
			try {
				int n = 0;
				for (int i = 0;; i++) {
					String s = data(i);
					bw.write(s);
					bw.write('\n');
					n += s.getBytes("UTF-8").length + 1;
					if (n >= size) {
						break;
					}
				}
			} finally {
				bw.close();
			}
		} finally {
			os.close();
		}
		return 0;
	}

	protected abstract String data(int i);
}
