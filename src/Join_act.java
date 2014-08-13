import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join_act {

	public static class joinMap extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 按行读取数据
			String str = value.toString();

			// 区分左右表
			String[] s = str.split(";");

			if (s.length > 5) {
				if (str.contains("dirtable")) {
					String[] dname = s[4].split(",");
					word.set("R|" + s[0]);
					for (int i = 0; i < dname.length; i++) {
						context.write(new Text(dname[i]), word);
					}
				} else {
					String[] aname = s[4].split(",");
					word.set("L|" + s[0]);
					for (int j = 0; j < aname.length; j++) {
						context.write(new Text(aname[j]), word);
					}
				}
			}
			// else{
			// context.write(new Text(str), new Text(s[0]));
			// }
		}
	}

	public static class joinReduce extends Reducer<Text, Text, Text, Text> {
		// reduce解析map输出，将value中数据按照左右表分别保存，然后等值连接，输出
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> rightlist = new ArrayList<String>();
			ArrayList<String> leftlist = new ArrayList<String>();

			int rightnum = 0;
			int leftnum = 0;
			String joinout = new String();

			// 按行读取数据,区分左右表
			for (Text it : values) {
				if (it.toString().charAt(0) == 'R') {
					String str = it.toString();
					String[] dn = str.split("\\|");
					String[] dname = dn[1].split(",");
					for (int i = 0; i < dname.length; i++) {
//						if (i == 0) {
//							rightlist.add("director : " + dname[i]);
//						}
						rightlist.add("director : " + dname[i]);
						//context.write(key, new Text(String.valueOf(rightlist.get(i))));
					}
				} else if (it.toString().charAt(0) == 'L') {
					String str = it.toString();
					String[] an = str.split("\\|");
					String[] aname = an[1].split(",");
					for (int j = 0; j < aname.length; j++) {
//						if (j == 0) {
//							leftlist.add("actor : " + aname[j]);
//						}
						leftlist.add("actor : " + aname[j]);
						//context.write(key, new Text(String.valueOf(rightlist.get(j))));
					}
				}
			}

			// 左右表进行等值连接
			for (int i = 0; i < leftlist.size(); i++) {
				for (int j = 0; j < rightlist.size(); j++) {
					joinout = String.valueOf(leftlist.get(i)) + "=="
							+ String.valueOf(rightlist.get(j));
					context.write(key, new Text(joinout));
				}
				//context.write(key, new Text(new String("!!!!!11")));
			}// end for
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "dir_act join");
		job.setJarByClass(Join_act.class);
		job.setMapperClass(joinMap.class);
		job.setNumReduceTasks(4); // must change 大分区个数
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