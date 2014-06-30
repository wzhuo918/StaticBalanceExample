import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.*;

public class mypartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		// TODO Auto-generated method stub

		int mircopartitionnumber = 250; // 每次job需要修改，值为小partition的总数
		int result = -1;
		int tmp;
		int sign = 0;

		tmp = Integer.parseInt(key.toString());

		// 自定义的均匀分布的hash函数
		if (sign == 0) {
			result = (int) (tmp % mircopartitionnumber);
			sign = 1;
		}

		return result;
	}
}
