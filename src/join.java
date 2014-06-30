import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class join {

	public static class joinMap extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 按行读取数据
			String str = value.toString();

			// 找出数据中的分割点
			String[] s = str.split("\\|");
			if (s.length == 1) {
				// lineitem表,右表，第二列为partkey
				word.set(s[0]);
				context.write(word, new Text("R|" + word));
			} else {
				// 表,左表，第一列
				word.set(s[0]);
				context.write(word, new Text("L|" + word));
			}
		}
	}

	public static class joinReduce extends Reducer<Text, Text, Text, Text> {
		// reduce解析map输出，将value中数据按照左右表分别保存，然后等值连接，输出
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<Integer> rightlist = new ArrayList<Integer>();
			ArrayList<Integer> leftlist = new ArrayList<Integer>();

			int rightnum = 0;
			int leftnum = 0;

			// 按行读取数据,区分左右表
			for (Text it : values) {
				if (it.toString().charAt(0) == 'R') {
					String str = it.toString();
					String[] s = str.split("\\|");
					rightlist.add(Integer.parseInt(s[1]));
				} else if (it.toString().charAt(0) == 'L') {
					String str = it.toString();
					String[] s = str.split("\\|");
					leftlist.add(Integer.parseInt(s[1]));
				}
			}

			// 左右表进行等值连接
			for (int i = 0; i < leftlist.size() - 1; i++) {
				for (int j = 0; j < rightlist.size() - 1; j++) {
					if (leftlist.get(i) == rightlist.get(j)) {
						context.write(key,
								new Text(String.valueOf(leftlist.get(i))));
					}
				}
			}// end for
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "native join");
		job.setJarByClass(join.class);
		job.setMapperClass(joinMap.class);
		job.setNumReduceTasks(40); // must change 大分区个数
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(joinReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}