package sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 直接输入URL作为查询关键词的搜索所占的比例
 */
public class Mr_5 extends Configured implements Tool {
	/*
	 * 初步切割，抽取
	 */
	public static class SogouMapper extends Mapper<Object, Text, Text, Text> {
		private Text url = new Text();
		private Text tmp = new Text("tmp");
		
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String[] arr = value.toString().split("\t", -1);
			
			if (arr.length == 6) {
				url.set(arr[2]);
				context.write(tmp, url);
			}
		}
	}
	
	/*
	 * 利用数据计算结果
	 */
	public static class SogouReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			double sum = 0;
			double sum1 = 0;
			double sum2 = 0;
			for (Text value : values) {
				if (value != null && value.toString().matches("^(http://|https://){0,1}([A-Za-z]+\\.){0,1}[A-Za-z0-9]+(\\.[A-Za-z]+){1,}$")) {
					sum ++;
				}
				sum1 ++;
			}
			sum2 = sum/sum1;
			key.set("直接输入URL作为查询关键词的搜索所占的比例:	");
			result.set(sum2);
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Mr_5.class);
		job.setMapperClass(SogouMapper.class);
		job.setReducerClass(SogouReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_5(), args);
		System.exit(exitCode);
	}
}
