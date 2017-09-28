package sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * 查询次数大于2次的用户总数
 */
public class Mr_1 extends Configured implements Tool {	
	/*
	 * 初步切词，抽取
	 */
	public static class LineSplitMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text uid = new Text();
		private IntWritable one = new IntWritable(1);

		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t", -1);
			if (arr.length == 6) {
				uid.set(arr[1]);
				context.write(uid, one);
			}		
		}
	}

	/*
	 * 统计搜索次数
	 */
	public static class SumReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/*
	 * 过滤查询次数大于10次的用户
	 */
	public static class LineFilterMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text people = new Text("people");
		private IntWritable one = new IntWritable(1);

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t", -1);
			int count = Integer.parseInt(arr[1]);
			if (arr.length == 2 && count > 10) {
				context.write(people, one);
			}
		}
	}

	/*
	 * 最终统计
	 */
	public static class SumReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			int sum = 0;
			for (IntWritable value : values) {
				sum += 1;
			}
			result.set(sum);
			context.write(key, result);
		}	
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = new Job();
		job1.setJarByClass(Mr_1.class);
		job1.setMapperClass(LineSplitMapper.class);
		job1.setReducerClass(SumReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.21:9000/mr_1"));
		job1.waitForCompletion(true);
		Job job2 = new Job();
		job2.setJarByClass(Mr_1.class);
		job2.setMapperClass(LineFilterMapper.class);
		job2.setReducerClass(SumReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.21:9000/mr_1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new Mr_1(), args);
		System.exit(exitCode);
	}
}
