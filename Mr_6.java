package sogou;

import java.io.IOException;

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
 * 直接输入URL作为关键词的查询中，点击结果（即URL字段）
 * 就是用户输入URL关键词的查询比例（这里只看域名domain，只要domain相同就认为相同）
 */
public class Mr_6 extends Configured implements Tool {
	/*
	 * 初步切割，抽取
	 */
	public static class SogouMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		//	private Text url = new Text();

		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String arr[] = value.toString().split("\t", -1);
			if (arr.length == 6) {
				word.set(arr[2]);
			}
			context.write(word, value);
		}
	}

	/*
	 * 匹配，进行数量统计
	 */
	public static class SogouReducer extends Reducer<Text, Text, Text, Text> {
		double sum1 = 0;
		double sum2 = 0;
		private Text key= new Text("result:	");
		private Text result=new Text();

		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			//	String Skey = key.toString();
			for (Text value : values) {
				String [] arr=value.toString().split("\t",-1);
				String word=arr[2];
				String url=arr[5];
				if(arr.length == 6) {
					if (word.matches("^(http://|https://){0,1}([A-Za-z]+\\.){0,1}[A-Za-z0-9]+(\\.[A-Za-z]+){1,}$")) {
						sum1 ++;
						if (url.toString().indexOf(word) != -1) {
							sum2 ++;
						}
					}
				}
			}
		}

		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double per=(sum2/sum1)*100;
			result.set("直接输入URL作为关键词的个数："+sum1+"\t"+"关键字与URL匹配的个数："+sum2+"\t"+"所占百分比为："+per+"%");
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(Mr_6.class);
		job.setMapperClass(SogouMapper.class);
		job.setReducerClass(SogouReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Mr_6(), args);
		System.exit(exitCode);
	}
}
