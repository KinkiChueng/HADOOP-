package lianxi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

public class PvUv extends Configured implements Tool {
	
	public static class SplitMapper extends Mapper<Object, Text, Text, Text> {
		private Text uri = new Text();
		private Text uid = new Text();
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arr = value.toString().split("\t");
			
			if (arr.length == 11) {
				uid.set(arr[3]);
				uri.set(arr[0]);
				context.write(uri, uid);
			}
		}
		
	}
	
	public static class SumReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Set<Text> list = new HashSet<Text>();
			int uv = 0, pv = 0;
			for (Text value : values) {
				if (list.contains(value)) {
					pv ++;
				} else {
					list.add(value);
				}
				uv++;			
			}
			String a = "pv: " + pv + "\tuv：" + uv;
			System.out.println("pv:" + pv);
			System.out.println("uv:" + uv);
			context.write(key, new Text(a));
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(PvUv.class);
		job.setMapperClass(SplitMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PvUv(), args);
		System.exit(exitCode);
	}
}
