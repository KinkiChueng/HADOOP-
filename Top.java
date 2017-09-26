package lianxi;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

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
 * 1）计算uri访问top10
 * @author Administrator
 *
 */
public class Top extends Configured implements Tool {

	public static class SplitMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text uri = new Text();
		private IntWritable one = new IntWritable(1);
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arr = value.toString().split("\t", -1);

			if (arr.length == 11) {
				//	System.out.println(arr[0]);
				uri.set(arr[0]);
				context.write(uri, one);
			}
		}
	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}			
			context.write(key, new IntWritable(sum));
		}

	}

	public static class TopTenMapper extends Mapper<Object, Text, IntWritable, Text> {
		private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, IntWritable, Text>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arr = value.toString().split("\t", -1);
			String uri = arr[0];
			int num = Integer.parseInt(arr[1]);
			repToRecordMap.put(num, uri);

			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}

		}
		@Override
		protected void cleanup(
				Mapper<Object, Text, IntWritable, Text>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Map.Entry<Integer, String> entry : repToRecordMap.entrySet()) {
				System.out.println(entry.getKey() + "\t" + entry.getValue());
				//	if (entry.getKey()!=null && entry.getValue()!=null)
				context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
			}

		}
	}

	public static class TopTenReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		@Override
		protected void cleanup(
				Reducer<IntWritable, Text, Text, NullWritable>.Context context)
						throws IOException, InterruptedException {
			for (Text t : repToRecordMap.descendingMap().values()) {
				// Output our ten records to the file system with a null key
				context.write(t, NullWritable.get());
			}
		}
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, NullWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values) {
				//	Map<String, String> parsed = transformXmlToMap(value.toString());
				repToRecordMap.put(key.get(),
						new Text(value));
				// If we have more than ten records, remove the one with the lowest rep
				// As this tree map is sorted in descending order, the user with
				// the lowest reputation is the last key.
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Top.class);
		job.setMapperClass(SplitMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.50:9000/top"));	
		job.waitForCompletion(true);

		Job job1 = new Job();
		job1.setJarByClass(Top.class);
		job1.setMapperClass(TopTenMapper.class);
		job1.setReducerClass(TopTenReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job1, new Path("hdfs://192.168.1.50:9000/top"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));	
		return job1.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Top(), args);
		System.exit(exitCode);
	}
}

