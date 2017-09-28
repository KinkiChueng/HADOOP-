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
 * 搜索过“%仙剑奇侠传%”（模糊匹配），并且查询次数大于10 的UID
 */
public class Mr_7 extends Configured implements Tool {
	/*
	 * 初步切割，抽取,匹配
	 */
	public static class SogouMapper extends Mapper<Object, Text, Text, Text> {
		private Text name = new Text();

		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			String arr[] = value.toString().split("\t", -1);
			if (arr.length == 6) {
				if (arr[2].indexOf("仙剑奇侠传") != -1) {
					name.set(arr[1]);
					context.write(name, value);					
				}
			}
		}
	}

	/*
	 * 判断满足条件的传出
	 */
	public static class SogouReducer extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable num = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				sum ++;
			}
			if (sum >= 10) {
				num.set(sum); 
				context.write(key, num);
			}	
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Mr_7.class);
		job.setMapperClass(SogouMapper.class);
		job.setReducerClass(SogouReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_7(), args);
		System.exit(exitCode);
	}
}
