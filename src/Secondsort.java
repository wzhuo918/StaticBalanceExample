import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class Secondsort extends Configured implements Tool {

//自定义的类型
    static class IntPair implements WritableComparable<IntPair> {
        private int a;
        private int b;

        public IntPair() {
            a = 0;
            b = 0;
        }

        public int getA() {
            return a;
        }

        public void setA(int a) {
            this.a = a;
        }

        public int getB() {
            return b;
        }

        public void setB(int b) {
            this.b = b;
        }

        public void set(int a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public int compareTo(IntPair o) {
            if (this.a == o.a) {
                if (this.b == o.b)
                    return 0;
                else
                    return this.b > o.b ? 1 : -1;
            } else
                return this.a > o.a ? 1 : -1;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(a);
            dataOutput.writeInt(b);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            a = dataInput.readInt();
            b = dataInput.readInt();
        }
    }

    static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("	");
            if(input.length == 2){
            	StringTokenizer strTok = new StringTokenizer(value.toString());
                int a = Integer.parseInt(strTok.nextToken());
                int b = Integer.parseInt(strTok.nextToken());
                IntPair mykey = new IntPair();
                mykey.set(a, b);
                context.write(mykey, new IntWritable(b));            	
            }
        }
    }

    static class MyKeyGroupComparator extends WritableComparator {
        MyKeyGroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            if (ip1.a == ip2.a)
                return 0;
            else
                return ip1.a > ip2.a ? 1 : -1;
        }
    }

    static class Reduce extends Reducer<IntPair, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable myKey = new IntWritable(key.getA());
          //显式的分隔分组，便于查看
            //context.write(new IntWritable(999999999), null);
            for(IntWritable i :values){
                context.write(myKey, i);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //path是HDFS的路径字符串
//        String path = "/my/inputTest/Test_SecondSort.txt";
//        if (strings.length != 1) {
//            System.out.println("input:" + path);
//            System.out.print("arg:<out>");
//            return 1;
//        }
        Configuration conf = getConf();
        Job job = new Job(conf, "native SecondSort  r20");
        job.setJarByClass(Secondsort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setGroupingComparatorClass(MyKeyGroupComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int rst = ToolRunner.run(conf, new Secondsort(), args);
        System.exit(rst);
    }
}
