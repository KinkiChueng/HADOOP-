package lianxi;

import java.io.IOException;
import java.util.Random;

import lianxi.PvUv.SplitMapper;
import lianxi.PvUv.SumReducer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class sampling extends Configured implements Tool {

	public static class SplitMapper extends Mapper<Object, Text, Text, NullWritable> {
		Random rand = new Random();
	//	private Double percentage;

//		@Override
//		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			String strPercentage = context.getConfiguration()
//					.get("filter_percentage");
//			percentage = Double.parseDouble(strPercentage) / 97.0;
//
//		}

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			double tmp = rand.nextDouble();
			double i = 1L;
			double j = 97L;
			double percent = i/j;;
			//System.out.println(percent);
			if (tmp <= percent) {
				System.out.println("aa" + tmp);
				context.write(value, NullWritable.get());
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(sampling.class);
		job.setMapperClass(SplitMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new sampling(), args);
		System.exit(exitCode);
	}
}
