import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class prodata {

	public static class proMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		//private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();
		
		long length;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			String str = value.toString();
			length = length + value.getLength() + 1;
			
			str=str.replace("#", "");
			str=str.replace("&", "");
			str=str.replace("{", "");
			str=str.replace("}", "");
			str=str.replace("@", "");
			str=str.replace("?", "");
			
			//str=str.replace(", \"movies\" : [ ", "; movies ;");
			//r=str.replace("], \"moviesSize\" :", "; moviesSize ;");

			
			//str=str.replace(",,[,", ",");
			//str=str.replace(",,", ",");
			
//			while(  (str.charAt(0) < 'A') || ((str.charAt(0) > 'Z') & (str.charAt(0) < 'a')) || (str.charAt(0) > 'z') ){
//				str=str.replace(str.subSequence(0, 1), "");
//			}
			
			context.write(new Text(str), NullWritable.get());
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
		job.setJarByClass(prodata.class);
		job.setMapperClass(proMapper.class);
		//job.setNumReduceTasks(2);
		//job.setReducerClass(proReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
