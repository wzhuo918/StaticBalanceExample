import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class pagerank {

	public static enum counter// third
	{
		// 记录已经收敛的个数
		Map, num
	};

	public static class PageRankMap extends
			Mapper<Object, Text, LongWritable, Text> {

		/**
		 * 每次job需要初始化
		 */
		public int sumnum_update = 80000; // 隔多少条记录更新统计信息
		public int mircopartitionnumber = 250; // must change 子分区总个数

		public int num_update = 0; // 采样统计量的初始化
		private Text word = new Text();
		int length;
		FileSplit sp = null;
		TaskTrackerServerSocket client = new TaskTrackerServerSocket(); // 发送统计信息

		public int[] count = new int[mircopartitionnumber];
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
			int tmp;
			int sign = 0;
			tmp = Integer.parseInt(id1.toString());

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
				Map<String, String> PartitionValueNum, int[] tmp) {
			// must changes 子分区个数
			for (int i = 0; i < mircopartitionnumber; i++) {
				PartitionValueNum
						.put(String.valueOf(i), String.valueOf(tmp[i]));
			}
		}

		// 存储网页ID
		private LongWritable id;
		// 存储网页PR值
		private String pr;
		// 存储网页向外链接总数目
		private long pcount;
		// 网页向每个外部链接的平均贡献值
		private float average_pr;

		/**
		 * @see 在map中先区分输入行属于左表还是右表，然后对两列值进行分割
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 初始化partition
			if (sp == null) {
				sp = (FileSplit) context.getInputSplit();
				sp.getLength();
				this.PartitionValueIdInit(PartitionValueId);
				this.PartitionValueNumInit(PartitionValueNum);
			}

			StringTokenizer str = new StringTokenizer(value.toString());

			// System.out.println("str!!!" + str);
			// System.err.append("err!!!" + str);

			if (str.hasMoreTokens()) {
				// 得到网页ID
				id = new LongWritable(Long.parseLong(str.nextToken()));
			} else {
				return;
			}

			word.set(new Text(String.valueOf(id)));
			// System.out.println("word!!!" + word);

			// word.set(new Text(String(id)));

			// 非空进行map方法
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

				// 得到网页pr
				pr = str.nextToken();
				// 得到向外链接数目
				pcount = str.countTokens();
				// 对每个外部链接平均贡献值
				average_pr = Float.parseFloat(pr) / pcount;
				// 得到网页的向外链接ID并输出
				while (str.hasMoreTokens()) {
					try {
						String nextId = str.nextToken();
						// 将网页向外链接的ID以“@+得到贡献值”格式输出
						Text t = new Text("@" + average_pr);

						// context.write(id, t);//src

						context.write(new LongWritable(Long.parseLong(nextId)),
								t);// xiugai

						// 将网页ID和PR值输出
						Text tt = new Text("#" + nextId);

						context.write(id, tt);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					Text t = new Text("$" + pr);
					try {
						context.write(id, t);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}// endwile
			}
		}
	}// end map

	/**
	 * @see reduce解析map输出，将value中数据按照左右表分别保存，然后求笛卡尔积，输出
	 * @author wzhuo
	 * 
	 */
	public static class PageRankReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) {
			// 上次的PR值
			double srcpr = 0;

			// 定义一个存储网页链接ID的队列
			ArrayList<String> ids = new ArrayList<String>();
			// 将所有的链接ID以String格式保存
			String lianjie = "";
			// 定义一个保存网页PR值的变量
			float pr = 0;
			// 遍历
			for (Text id : values) {
				String idd = id.toString();
				// 判断value是贡献值还是向外部的链接
				if (idd.substring(0, 1).equals("@")) {
					// 贡献值
					pr += Float.parseFloat(idd.substring(1));
				} else if (idd.substring(0, 1).equals("#")) {
					// 链接id
					String iddd = idd.substring(1);
					// System.out.println("idddd= " + iddd);
					ids.add(iddd);
				} else if (idd.substring(0, 1).equals("$")) {
					srcpr = Double.parseDouble(idd.substring(1));
				}
			}
			// 计算最终pr

			pr = pr * 0.85f + 0.15f;
			if (Math.abs(srcpr - pr) < 0.1) {
				context.getCounter(counter.num).increment(1);
			}
			// 得到所有链接ID的String形式
			for (int i = 0; i < ids.size(); i++) {
				lianjie = lianjie + ids.get(i) + " ";
			}
			// 组合pr+lianjie成原文件的格式类型
			String result = "\t" + pr + "\t" + lianjie;
			// System.out.println("Reduce    result=" + result);
			try {
				context.write(key, new Text(result));
				// System.out.println("reduce 执行完毕。。。。。");
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param arg
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String pathIn1 = "/user/wzhuo/pageranktwitter";// 输入路径
		String pathOut01 = "/user/wzhuo/pageranktwitter_out01";// 输出路径
		String pathOut02 = "/user/wzhuo/uk100pagerank_out02";// 输出路径

		String temp = "";
		FileSystem.setDefaultUri(conf, new URI("hdfs://172.19.0.164:9000"));// jia

		for (int i = 0; i < 1; i++) {
			// System.out.println("xunhuan cishu=" + i);
			Job job = new Job(conf, "our uk 50x5");
			// pathOut = pathIn1 + i;
			job.setJarByClass(pagerank.class);
			job.setMapperClass(PageRankMap.class);
			job.setPartitionerClass(mypartitioner.class);
			job.setReducerClass(PageRankReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(50); // must change 大分区个数
			if (i == 0) {
				FileInputFormat.addInputPath(job, new Path(pathIn1));
				FileOutputFormat.setOutputPath(job, new Path(pathOut01));
				// FileOutputFormat.setOutputPath(job, new Path(pathOut02));

				job.waitForCompletion(true);
			} else {
				FileInputFormat.addInputPath(job, new Path(pathOut01));
				FileOutputFormat.setOutputPath(job, new Path(pathOut02));

				job.waitForCompletion(true);

				FileSystem.get(job.getConfiguration()).delete(
						new Path(pathOut01), true);// 如果文件已存在删除, second
				temp = pathOut01;// second
				pathOut01 = pathOut02;
				pathOut02 = temp;// second
			}

			// third after
			Counters counter = job.getCounters();
			int count = (int) counter.findCounter(pagerank.counter.num)
					.getValue();
			System.out.println("count=" + count);
			if (count != 61578414)
				counter.findCounter(pagerank.counter.num).increment(0 - count);
			if (count == 61578414)
				break; // 代表是个网页都收敛拉

		}
	}
}