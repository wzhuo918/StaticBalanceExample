/******************************************************
 * input.txt文件输入格式如下：
网页ID 初始PR值 外连接网页ID
1          1                      5  3  2  4 
2          1                   8  3  5  9 
3          1                      4 
4         1                   5  9  6 
5         1                      3  8 
6         1                    8  2  9 
7         1                      4  6  2 
8        1                        2  1 
9         1                    7  8  1 
10      1                     4  6  3 
------------------------------------------------
得到的文件结果如下：
网页ID 最终PR值 外连接网页ID
1        1.0442384  5  3  2  4 
2        1.3227271  8  3  5  9 
3        1.1596749  4 
4        1.5394952  5  9  6 
5        1.1113384  3  8 
6       0.76241493  8  2  9 
7       0.442547  4  6  2 
8      1.3829739  2  1 
9      1.0845904  7  8  1 
10     0.15  4  6  3  
 ***********************************************************
 */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Pagerank {
	public static enum counter// third
	{
		// 记录已经收敛的个数
		Map, num
	};

	public static class PageRankMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		// 存储网页ID
		private IntWritable id;
		// 存储网页PR值
		private String pr;
		// 存储网页向外链接总数目
		private int count;
		// 网页向每个外部链接的平均贡献值
		private float average_pr;

		public void map(Object key, Text value, Context context) {
			StringTokenizer str = new StringTokenizer(value.toString());
			//System.out.println("str!!!"+str);
			if (str.hasMoreTokens()) {
				// 得到网页ID
				id = new IntWritable(Integer.parseInt(str.nextToken()));
			} else {
				return;
			}
			// 得到网页pr
			pr = str.nextToken();
			// 得到向外链接数目
			count = str.countTokens();
			// 对每个外部链接平均贡献值
			average_pr = Float.parseFloat(pr) / count;
			// 得到网页的向外链接ID并输出
			while (str.hasMoreTokens()) {
				try {
					String nextId = str.nextToken();
					// 将网页向外链接的ID以“@+得到贡献值”格式输出
					Text t = new Text("@" + average_pr);

					//System.out.println("nextId!!!"+nextId);
					// context.write(id, t);//src
					context.write(new IntWritable(Integer.parseInt(nextId)), t);// xiugai

					// 将网页ID和PR值输出
					Text tt = new Text("#" + nextId);
					//System.out.println("tt!!!"+tt);
					context.write(id, tt);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				Text t = new Text("$" + pr);
				try {
					//System.out.println("t!!!"+t);
					context.write(id, t);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}//end while
		}
	}

	public static class PageRankReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) {
			// 上次的PR值
			double srcpr = 0;

			// 定义一个存储网页链接ID的队列
			ArrayList<String> ids = new ArrayList<String>();
			// 将所有的链接ID以String格式保存
			String lianjie = "  ";
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
					//System.out.println("idddd= " + iddd);
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
				lianjie = lianjie + ids.get(i) + "  ";
			}
			// 组合pr+lianjie成原文件的格式类型
			String result = pr + lianjie;
			System.out.println("Reduce    result=" + result);
			try {
				context.write(key, new Text(result));
				System.out.println("reduce 执行完毕。。。。。");
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, URISyntaxException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String pathIn1 = "/user/wzhuo/pageranktwitter";// 输入路径
		String pathOut01 = "/user/wzhuo/pageranktwitter_twitterout01";// 输出路径
		String pathOut02 = "/user/wzhuo/ukdatapagerank_ukout02";// 输出路径
		
		String temp = "";
		FileSystem.setDefaultUri(conf, new URI("hdfs://172.19.0.164:9000"));// jia

		for (int i = 0;i<1; i++) {
			System.out.println("xunhuan =" + i);
			Job job = new Job(conf, "native pagerank r20 uk");
			// pathOut = pathIn1 + i;
			job.setJarByClass(Pagerank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(50); // must change 大分区个数
			if(i==0){
				FileInputFormat.addInputPath(job, new Path(pathIn1));
				FileOutputFormat.setOutputPath(job, new Path(pathOut01));
				//FileOutputFormat.setOutputPath(job, new Path(pathOut02));
				
				job.waitForCompletion(true);
			}
			else{
				FileInputFormat.addInputPath(job, new Path(pathOut01));
				FileOutputFormat.setOutputPath(job, new Path(pathOut02));	
				
				job.waitForCompletion(true);
				
				FileSystem.get(job.getConfiguration()).delete(new Path(pathOut01),
						true);// 如果文件已存在删除, second
				temp = pathOut01;// second
				pathOut01 = pathOut02;
				pathOut02 = temp;// second
			}
			

			// third after
			Counters counter = job.getCounters();
			int count = (int) counter.findCounter(Pagerank.counter.num)
					.getValue();
			System.out.println("count=" + count);
			if (count != 61578414)
				counter.findCounter(Pagerank.counter.num).increment(0 - count);
			if (count == 61578414)
				break; // 代表是个网页都收敛拉

		}
	}
}