package sogou;

import java.io.IOException;
import java.util.ArrayList;

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
 * Rank在10（包含）以内的搜索次数占比，Rank是第4个Int类型字段
 */
public class Mr_8 extends Configured implements Tool {
	/*
	 * 初步切割，抽取,匹配
	 */
	public static class SogouMapper1 extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		private IntWritable rank = new IntWritable();
		private DoubleWritable one = new DoubleWritable(1);
		@Override
		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String arr[] = value.toString().split("\t", -1);
			if (arr.length == 6) {
				int r_temp = Integer.parseInt(arr[3]);
				//	if (r_temp <= 10) {
				rank.set(r_temp);
				//count ++;
				//	}
				context.write(rank, one);
			}
		}
	}

	public static class SogouReducer1 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		double k_sum = 0;
		//	static double sum = 0;
		private DoubleWritable end = new DoubleWritable();

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (DoubleWritable value : values) {
				//	if (key.get() <= 10) {
				k_sum += value.get();
				//}
			}
			//	count++;
			end.set(k_sum);
			//num.set(key.get());
			context.write(key, end);
		}
	}

	public static class SogouMapper2 extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String arr[] = value.toString().split("\t", -1);
			context.write(new IntWritable(Integer.parseInt(arr[0])), new DoubleWritable(Double.parseDouble(arr[1])));
		}
	}

	public static class SogouReducer2 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		double sum = 0;
		//double sum2 = 0;
		private DoubleWritable end = new DoubleWritable();
		ArrayList<Double> array = new ArrayList<Double>(); 
		@Override
		protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (DoubleWritable value : values) {
				if (key.get() <= 10) {
					//	sum1 += value.get();
					//	array.add(value.get());
					sum = value.get();
					end.set(sum/5000000);
					context.write(key, end);
				}
				sum += value.get();
			}
		}
//
//		protected void cleanup(Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			int i = 1;
//			for (Double temp : array) {
//				end.set(temp/5000000);
//				context.write(new IntWritable(i++), end);
//			}
//		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_8(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = new Job();
		job1.setJarByClass(Mr_8.class);
		job1.setMapperClass(SogouMapper1.class);
		job1.setReducerClass(SogouReducer1.class);
	//	job1.setNumReduceTasks(1);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.21:9000/mr_8"));
		//System.exit(job.waitForCompletion(true)?0:1);

		job1.waitForCompletion(true); //执行

		Job job2 = new Job();
		job2.setJarByClass(Mr_8.class);
		job2.setMapperClass(SogouMapper2.class);
		//	job2.setCombinerClass(SogouReducer2.class);
		job2.setReducerClass(SogouReducer2.class);
		job2.setNumReduceTasks(1);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.21:9000/mr_8"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		return job2.waitForCompletion(true)?0:1; //执行
	}
}
