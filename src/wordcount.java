import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount {

	public static class countMap extends
			Mapper<Object, Text, Text, IntWritable> {

		/**
		 * 每次job需要初始化
		 */
		public int sumnum_update = 80000; // 隔多少条记录更新统计信息
		public int mircopartitionnumber = 300; // must change 子分区总个数

		public long num_update = 0; // 采样统计量的初始化
		private final IntWritable one = new IntWritable(1);

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
			// IntWritable one = new IntWritable(1);

			String str = value.toString();
			length = length + value.getLength() + 1;
			String[] s = str.split(",");

			// 输入文件首行，不处理
			if (str.contains("Year") == true || str.contains("Month") == true) {
				return;
			}

			// 初始化partition
			if (sp == null) {
				sp = (FileSplit) context.getInputSplit();
				sp.getLength();
				this.PartitionValueIdInit(PartitionValueId);
				this.PartitionValueNumInit(PartitionValueNum);
			}

			for (int i = 0; i < s.length; i++) {
				if (i == 1 || i == 2 || i == 3 || i == 4 || i == 0 || i == 7
						|| i == 12 || i == 13 || i == 11 || i == 21 || i == 22
						|| i == 23 || i == 20 || i == 30) {

					word.set(s[i]);

					// 非空进行map方法
					if (PartitionValueId.isEmpty() == true) {
						System.out.println("PartitionValueId is NULL!");
						System.exit(0);
					} else {
						int partitionNum;
						partitionNum = PartitionValueIdContains(
								PartitionValueId, word.toString());

						// 统计同一partition值的数据量
						if (partitionNum != -1) {
							count[partitionNum]++;
							num_update++;
						}

						// 对采样的数据进行一次更新和发送，每隔sumnum_update条记录
						if ((num_update >= sumnum_update)
								|| (length == sp.getLength())) {
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

						// 找出数据中的分割点;将输入数据分为左右表分别存储
						context.write(word, one);
					}
				}// end if
			}
		}
	}// end joinmap

	/**
	 * @see reduce解析map输出，将value中数据按照左右表分别保存，然后求笛卡尔积，输出
	 * @author wzhuo
	 * 
	 */
	public static class countReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * @param arg
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Job job = new Job(conf, "our word 60");
		job.setJarByClass(wordcount.class);
		job.setMapperClass(countMap.class);
		job.setNumReduceTasks(60); // must change 大分区个数
		job.setPartitionerClass(mypartitioner.class);
		job.setReducerClass(countReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}