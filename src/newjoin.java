import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class newjoin {

	public static class JoinMap extends Mapper<Object, Text, Text, Text> {

		/**
		 * 每次job需要初始化
		 */
		public int sumnum_update = 200000; // 隔多少条记录更新统计信息
		public int mircopartitionnumber = 200; // must change 子分区总个数

		public long num_update = 0; // 采样统计量的初始化
		// private final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		long length;
		FileSplit sp = null;
		TaskTrackerServerSocket client = new TaskTrackerServerSocket(); // 发送统计信息

		public long[] count = new long[mircopartitionnumber];
		private static Map<String, Set<String>> PartitionValueId = new HashMap<String, Set<String>>();
		private static Map<String, String> PartitionValueNum = new HashMap<String, String>();

		/**
		 * init PartitionValueId 该函数完成PartitionValueId的初始化功能
		 * 注意：有多少个分区，就初始化多少个PartitionValueId.put("n", set);
		 **/
		public void PartitionValueIdInit(
				Map<String, Set<String>> PartitionValueId) {

			for (int i = 1; i < mircopartitionnumber + 1; i++) {
				Set<String> set = new HashSet<String>();
				if (i == mircopartitionnumber) {
					set.add(String.valueOf(0));
				} else {
					set.add(String.valueOf(i));
				}

				PartitionValueId.put(String.valueOf(i), set);
			}

		}

		/**
		 * PartitionValueId contains the id1 -1: not contain id1 n:
		 * partitionvalue=n contains id1 这个跟自定义的partition函数一样 用来判断id1属于那个分区
		 */
		public int PartitionValueIdContains(
				Map<String, Set<String>> PartitionValueId, String id1) {

			// 自定义的hash函数
			int result = -1;
			long tmp;
			int sign = 0;
			tmp = Long.parseLong(id1.toString());

			// 自定义的均匀分布的hash函数
			if (sign == 0) {
				result = (int) (tmp % mircopartitionnumber);
				sign = 1;
			}

			return result;
		}

		/**
		 * init PartitionValueNum 用来存放《分区值，分区的元组数》 使用之前将其进行如下所示的初始化
		 */
		public void PartitionValueNumInit(Map<String, String> PartitionValueNum) {
			int partitionvalue = 1;
			String tmp = null;

			for (int j = 1; j < mircopartitionnumber + 1; j++) {
				tmp = null;
				if (partitionvalue == mircopartitionnumber)
					partitionvalue = 0;

				tmp = String.valueOf(partitionvalue);
				PartitionValueNum.put(tmp, "0");
				partitionvalue++;
			}
		}

		/**
		 * @see 初始化分区的统计量，使之全部为0；
		 * @param PartitionValueNum
		 * @param tmp
		 */
		public void PartitionValueNumUpdate(
				Map<String, String> PartitionValueNum, long[] tmp) {
			// must changes 子分区个数
			for (int i = 0; i < mircopartitionnumber; i++) {
				PartitionValueNum
						.put(String.valueOf(i), String.valueOf(tmp[i]));
			}
		}

		/**
		 * @see 在map中先区分输入行属于左表还是右表，然后对两列值进行分割
		 * @see 保存连接列在key值，剩余列和左右表标志在value中，最后输出
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 按行读取数据
			String str = value.toString();

			// 找出数据中的分割点
			String[] s = str.split("\\|");
			if (s.length == 1) {
				// lineitem表,右表，第二列为partkey
				word.set(s[0]);				
			} else {
				// partsupp表,左表，第一列为partkey
				word.set(s[0]);				
			}		

			// 初始化partition
			if (sp == null) {
				sp = (FileSplit) context.getInputSplit();
				sp.getLength();
				this.PartitionValueIdInit(PartitionValueId);
				this.PartitionValueNumInit(PartitionValueNum);
			}

			if (PartitionValueId.isEmpty() == true) {
				System.out.println("PartitionValueId is NULL!");
				System.exit(0);
			} else {
				int partitionNum;
				partitionNum = PartitionValueIdContains(PartitionValueId,
						word.toString());

				// 统计同一partition值的数据量
				if (partitionNum != -1) {
					count[partitionNum]++;
					num_update++;
				}

				// 对采样的数据进行一次更新和发送，每隔sumnum_update条记录
				if ((num_update >= sumnum_update) || (length == sp.getLength())) {
					PartitionValueNumUpdate(PartitionValueNum, count);
					try {
						String Message = null;
						Message = sp.toString() + "|"
								+ PartitionValueNum.toString();

						// 发送消息
						client.run(Message);
						num_update = 0;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				// 找出数据中的分割点
				if (s.length == 1) {
					// lineitem表,右表，第二列为partkey
					word.set(s[0]);
					context.write(word, new Text("R|" + word));
				} else {
					// zipf表,左表
					word.set(s[0]);
					context.write(word, new Text("L|" + word));
				}
			}
		}
	}// end joinmap

	/**
	 * @see reduce解析map输出，将value中数据按照左右表分别保存，然后求笛卡尔积，输出
	 * @author wzhuo
	 * 
	 */
	public static class JoinReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList rightlist = new ArrayList();
			ArrayList leftlist = new ArrayList();

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

		}// end reduce
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		Job job = new Job(conf, "new Join");
		job.setJarByClass(newjoin.class);
		job.setMapperClass(JoinMap.class);
		job.setNumReduceTasks(40); // must change 大分区个数
		job.setPartitionerClass(mypartitioner.class);
		job.setReducerClass(JoinReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		// job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}