package sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 直接输入URL作为查询关键词的搜索总量
 */
public class Mr_4 extends Configured implements Tool {
	/*
	 * 初步切词，抽取
	 */
	public static class LineSplitMapper extends Mapper<Object, Text, Text, Text> {
		private static final Text NullResult = new Text();
		private Text url = new Text();

		@Override
		protected void map(Object key, Text values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String[] arr = values.toString().split("\t", -1);
			if (arr.length == 6) {
				url.set(arr[2]);
			}
			context.write(NullResult, url);
		}
	}

	/*
	 * 进行匹配
	 */
	public static class SumReducer extends Reducer<Text, Text, NullWritable, IntWritable> {
		private NullWritable tmp = null;					//将text换为NullWritable
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (Text value : values) {
				if (value.toString().matches("^(http://|https://){0,1}([A-Za-z]+\\.){0,1}[A-Za-z0-9]+(\\.[A-Za-z]+){1,}$")) {
					sum ++;
				}
			}
			result.set(sum);
			context.write(tmp, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Mr_4.class);
		job.setMapperClass(LineSplitMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int editCode = ToolRunner.run(new Mr_4(), args);
		System.exit(editCode);
	}
}
