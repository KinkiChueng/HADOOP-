package sogou;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 查询次数大于10次的随机10个用户的完整数据展示（整条数据记录）
 */
public class Mr_3 extends Configured implements Tool {	
	/*
	 * 初步切词，抽取
	 */
	public static class LineSplitMapper1 extends Mapper<Object, Text, Text, Text> {
		private Text uid = new Text();
		private Text one = new Text();
		
		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t", -1);
			if (arr.length == 6) {
				uid.set(arr[1]);
				one.set(value.toString());
				context.write(uid, one);
			}
		}
	}

	/*
	 * 过滤搜索次数超过10次的
	 */
	public static class SumReducer1 extends Reducer<Text, Text, Text, Text> {
		private static final Text NullResult = null;

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum=0;
			ArrayList<Text> array = new ArrayList<Text>();
			for(Text value:values) {
				//sum += value.get();
				sum++;
				Text text= new Text();
				text.set(value.toString());
				array.add(text);
			}
			if (sum > 10) {
				int size = array.size();
				for (int i = 0; i < size; i++) {
					context.write(NullResult, array.get(i));
				}
			}
		}
	}

	/*
	 * 中间map
	 */
	public static class LineSplitMapper2 extends Mapper<Object, Text, Text, Text> {
		private Text get = new Text("get");
		
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			context.write(get, value);
		}
	}

	/*
	 * 显示十条
	 */
	public static class SumReducer2 extends Reducer<Text, Text, Text, Text> {
		private Text result = null;
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, Integer> map = new HashMap<String, Integer>(10);
			int n = 10;
			for (Text value:values) {
				if (n>0) {
					String [] arr = value.toString().split("\t", -1);
					String uid = arr[1];
					if (map.get(uid) != null) {				//用map控制保证得到十条数据
						//	context.write(result, value);
					} else {
						map.put(uid, 1);
						context.write(result, value);
						n --;
					}
				}
			}
		}	
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = new Job();
		job1.setJarByClass(Mr_3.class);
		job1.setMapperClass(LineSplitMapper1.class);
		job1.setReducerClass(SumReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.21:9000/mr_3"));
		job1.waitForCompletion(true);
		Job job2 = new Job();
		job2.setJarByClass(Mr_3.class);
		job2.setMapperClass(LineSplitMapper2.class);
		job2.setReducerClass(SumReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.21:9000/mr_3"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_3(), args);
		System.exit(exitCode);
	}
}
