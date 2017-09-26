package lianxi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mockito.exceptions.Reporter;

public class MapJoin extends Configured implements Tool  {

	public static class MapClass extends Mapper<Object, Text, Text, Text> {  

		//用于缓存小表的数据，在这里我们缓存info.txt文件中的数据  3
		private Map<String, String> joinData = new HashMap<String, String>();

		private Text outKey = new Text();  
		private Text outValue = new Text();  

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,   
				Reporter reporter) throws IOException  
		{  
			//首先获取order文件中每条记录的userId，  
			//再去缓存中取得相同userId的user记录，合并两记录并输出之。  
			String[] order = value.toString().split("\t");  
			String res = joinData.get(order[3]);  

			if(res != null)  
			{  
				outKey.set(value);  
				outValue.set(joinData.get(order[3]));  
				output.collect(outKey, outValue);  
			}  
		}
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 预处理把要关联的文件加载到缓存中
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());  
			System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"+DistributedCache.getLocalCacheFiles(context.getConfiguration()));
			// 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
			//System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"+paths[0].toString());
		    BufferedReader reader = new BufferedReader(new FileReader(paths[0].toString()));
		    String str = null;
			try {
				// 一行一行读取
				while ((str = reader.readLine()) != null) {
					// 对缓存中的表进行分割
					String[] splits = str.split("\t");
					// 把字符数组中有用的数据存在一个Map中
					joinData.put(splits[0], splits[1]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				reader.close();
			}
			//super.setup(context);
		}  

	}  

	public int run(String[] args) throws Exception  
	{   
		// 创建配置信息
		Configuration conf = new Configuration();
//		// 获取命令行的参数
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		// 当参数违法时，中断程序
//		if (otherArgs.length != 3) {
//			System.err.println("Usage:MapJoin<in1> <in2> <out>");
//			System.exit(1);
//		}

		// 给路径赋值
		String INPUT_PATH1 = args[0];
		String INPUT_PATH2 = args[1];
		String OUT_PATH = args[2];
		// 创建文件系统
		FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
		// 如果输出目录存在，我们就删除
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		// 添加到内存中的文件(随便添加多少个文件)
		DistributedCache.addCacheFile(new Path(INPUT_PATH2).toUri(), conf);

		// 创建任务
		Job job = new Job(conf, MapJoin.class.getName());				//重点
		// 打成jar包运行，这句话是关键
		job.setJarByClass(MapJoin.class);
		//1.1 设置输入目录和设置输入数据格式化的类
		FileInputFormat.setInputPaths(job, INPUT_PATH1);
		job.setInputFormatClass(TextInputFormat.class);

		//1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//1.3 设置分区和reduce数量
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 提交作业 退出
		return job.waitForCompletion(true) ? 0 : 1;
		
//		Job job = new Job();
//		job.setJarByClass(MapJoin.class);
//		job.setMapperClass(MapClass.class);
//		job.setNumReduceTasks(0);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//	//	DistributedCache.addCacheFile(new Path("hdfs://192.168.1.50:9000/mdata/info.txt").toUri(), conf);  
//		DistributedCache.addCacheFile(new Path("hdfs://192.168.1.50:9000/mdata/info.txt").toUri(), conf);
//		//DistributedCache.addLocalArchives(conf, str);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
//		return job.waitForCompletion(true)?0:1;
	}  

	public static void main(String[] args) throws Exception  
	{  
		int res = ToolRunner.run(new Configuration(), new MapJoin(), args);  
		
		System.exit(res);  
	}  

} 