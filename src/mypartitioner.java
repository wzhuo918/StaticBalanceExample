import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.*;

public class mypartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		// TODO Auto-generated method stub

		int mircopartitionnumber = 200; // 每次job需要修改，值为小partition的总数
		int result = -1;
		long tmp;
		int sign = 0;

		tmp = Long.parseLong(key.toString());

		// 自定义的均匀分布的hash函数
		if (sign == 0) {
			result = (int) (tmp % mircopartitionnumber);
			sign = 1;
		}

		return result;
	}

}
