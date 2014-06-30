
import java.io.IOException;
import java.util.StringTokenizer;

import javax.print.DocFlavor.STRING;

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

public class nativewordcount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		long length;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			String str = value.toString();
			length = length + value.getLength() + 1;
			
            //输入文件首行，不处理  
            if(str.contains("Year")==true||str.contains("Month")==true){  
                return;  
            }

			String[] s = str.split("\\.");
		
			int temp = 0;
			temp = Integer.valueOf(s[0]);
								
			String a = String.valueOf(temp);
			word.set(a);
			context.write(word, one);

//			for (int i = 0; i < s.length; i++) {
//				if (i == 1 || i == 2 || i == 3 || i == 4 || i == 0 || i == 7
//						|| i == 12 || i == 13 || i == 11 || i == 21 || i == 22
//						|| i == 23 || i == 20 || i == 30) {				
//						word.set(s[i]);
//						context.write(word, one);	
//				
//				}
//			}
		}
	}

	public static class IntSumReducer extends
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "wordcount virgodata");
		job.setJarByClass(nativewordcount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(20);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
