package sogou;

import java.io.IOException;
import java.util.ArrayList;

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
 * 列出有关仙剑奇侠传搜索次数最多的两条记录
 */
public class Mr_9 extends Configured implements Tool {
	/*
	 * 初步切割，抽取,匹配
	 */
	public static class SogouMapper1 extends Mapper<Object, Text, Text, Text> {
		private Text name = new Text();

		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String arr[] = value.toString().split("\t", -1);
			if (arr.length == 6) {
				if (arr[2].indexOf("仙剑奇侠传") != -1) {
					name.set(arr[2]);
					context.write(name, value);
				}
			}
		}
	}

	/*
	 * 将匹配结果存入list中进行统计
	 */
	public static class SogouReducer1 extends Reducer<Text, Text, Text, Text> {
		private  Text one=new Text("one");
		private  Text two=new Text("two");
		ArrayList<String> list1 = new ArrayList<String>();
		ArrayList<String> list2 = new ArrayList<String>();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> tmp = new ArrayList<String>();
			for (Text value : values) {
				tmp.add(value.toString());
			}
			if(tmp.size()>list2.size()){
				if(tmp.size()>list1.size()){
					list2.clear();
					list2.addAll(list1);
					list1.clear();
					list1.addAll(tmp);
				} else {
					list2.clear();
					list2.addAll(tmp);
				}
			}
		}

		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			String val1 = new String();
			String val2 = new String();
			for(String value:list1){
				val1 = value.split("\t", -1)[2];
			}
			for(String value:list2){
				val2 = value.split("\t", -1)[2];
			}
			context.write(one, new Text(val1 + "\t" + list1.size()));
			context.write(two, new Text(val2 + "\t" + list2.size()));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_9(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Mr_9.class);
		job.setMapperClass(SogouMapper1.class);
		job.setReducerClass(SogouReducer1.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
}
