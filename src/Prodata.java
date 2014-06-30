/**
 * 处理Twitter 数据
 * 初始格式：
 * user   follow
 * 1      3
 * 1      2
 *         
 * 处理后格式：
 * user 权值  follows
 *   1   1     3   2
 */
import java.io.IOException;

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

public class Prodata {

	public static class ProdataMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String str = value.toString();

			String[] s = str.split(" ");

			if (s.length == 2) {
				if ((!s[0].contains("%")) && (!s[1].contains("%")) && (!s[1].contains("83311")) && (!s[0].contains("83311"))) {
					word.set(s[0]);

					context.write(word, new Text(s[1]));
				}
			}
		}
	}

	public static class ProdataReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// LinkedList<Integer> vals = new LinkedList<Integer>();
			StringBuilder sb = new StringBuilder();
			sb.append("\t" + "1" + "\t");
			for (Text val : values) {
				sb.append(val.toString() + " ");
			}

			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "ProData UK 100");
		job.setJarByClass(Prodata.class);
		job.setMapperClass(ProdataMapper.class);
		job.setNumReduceTasks(100); // must change 大分区个数
		job.setReducerClass(ProdataReducer.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
