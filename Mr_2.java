package sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 查询次数大于10次的用户占总用户的比例
 */
public class Mr_2 extends Configured implements Tool {
	/*
	 * 初步切词，抽取
	 */
	public static class LineSplitMapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private Text uid = new Text();
		private IntWritable one = new IntWritable(1);
		
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String[] arr = value.toString().split("\t", -1);

			if (arr.length == 6) {
				uid.set(arr[1]);
				context.write(uid, one);
			}		
		}	
	}
	
	/*
	 * 统计各用户搜索次数
	 */
	public static class SumReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			int sum = 0;
			for (IntWritable value:values) {
				sum += 1;
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/*
	 * 将结果转换为double型
	 */
	public static class LineSplitMapper2 extends Mapper<Object, Text, Text, DoubleWritable> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String arr[] = value.toString().split("\t", -1);
			one.set(Double.parseDouble(arr[1]));
			context.write(people, one);
		}
		private Text people = new Text("percent");
		private DoubleWritable one = new DoubleWritable();
	}
	
	/*
	 * 统计计算
	 */
	public static class SumReducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//uper.reduce(arg0, arg1, arg2);
			double sum = 0;
			double sum1 = 0;
			double end = 0;
			for (DoubleWritable value : values) {
				if (value.get() > 10) {
					sum1 += 1;
				}
				sum += 1;
			}
			end = sum1 / sum;
			result.set(end);
			context.write(key, result);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = new Job();
		job1.setJarByClass(Mr_2.class);
		job1.setMapperClass(LineSplitMapper1.class);
		job1.setReducerClass(SumReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.21:9000/mr_2"));
		job1.waitForCompletion(true);
		Job job2 = new Job();
		job2.setJarByClass(Mr_2.class);
		job2.setMapperClass(LineSplitMapper2.class);
		job2.setReducerClass(SumReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.21:9000/mr_2"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_2(), args);
		System.exit(exitCode);
	}
}
