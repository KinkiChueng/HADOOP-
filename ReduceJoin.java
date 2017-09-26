package lianxi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoin extends Configured implements Tool {

	private static class CombineEntity implements WritableComparable<CombineEntity> {

		private Text joinKey;//连接key
		private Text flag;//文件来源标志
		private Text secondPart;//除了键外的其他部分的数据

		public CombineEntity() {
			// TODO Auto-generated constructor stub
			this.joinKey=new Text();
			this.flag=new Text();
			this.secondPart=new Text();
		}
		
		public Text getJoinKey() {
			return joinKey;
		}

		public void setJoinKey(Text joinKey) {
			this.joinKey = joinKey;
		}

		public Text getFlag() {
			return flag;
		}

		public void setFlag(Text flag) {
			this.flag = flag;
		}

		public Text getSecondPart() {
			return secondPart;
		}

		public void setSecondPart(Text secondPart) {
			this.secondPart = secondPart;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.joinKey.readFields(in);
			this.flag.readFields(in);
			this.secondPart.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			this.joinKey.write(out);
			this.flag.write(out);
			this.secondPart.write(out);
		}

		@Override
		public int compareTo(CombineEntity o) {
			// TODO Auto-generated method stub
			return this.joinKey.compareTo(o.joinKey);
		}	
	}

	public static class JoinMapper extends Mapper<Object, Text, Text, CombineEntity> {
		private CombineEntity combine=new CombineEntity();
		private Text flag=new Text();
		private  Text joinKey=new Text();
		private Text secondPart=new Text();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, CombineEntity>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//获得文件输入路径
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			System.out.println(pathName);
			if(pathName.endsWith(".sta.gz")) {
				String values[] = value.toString().split("\t");
				flag.set("0");
			//	System.out.println(values[3]);
				if(values.length == 11) {
					joinKey.set(values[3]);
					secondPart.set(value);
					//封装实体
					combine.setFlag(flag);//标志位
					combine.setJoinKey(joinKey);//链接键
					combine.setSecondPart(secondPart);//其他部分

					context.write(combine.getJoinKey(), combine);
				}
			} else if(pathName.endsWith("info.txt")) {
				String values[] = value.toString().split("\t");
				flag.set("1");
				joinKey.set(values[0]);
				secondPart.set(value);
				//封装实体
				combine.setFlag(flag);//标志位
				combine.setJoinKey(joinKey);//链接键
				combine.setSecondPart(secondPart);//其他部分

				//写出
				context.write(combine.getJoinKey(), combine);
			}
		}

	}

	public static class JoinReducer extends Reducer<Text, CombineEntity, Text, Text> {
		//存储一个分组中左表信息
		private List<Text> leftTable=new ArrayList<Text>();
		//存储一个分组中右表信息
		private List<Text> rightTable=new ArrayList<Text>();
		private Text secondPart=null;
		private Text output=new Text();
		@Override
		protected void reduce(Text key, Iterable<CombineEntity> values,
				Reducer<Text, CombineEntity, Text, Text>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			leftTable.clear();
			rightTable.clear();
			for (CombineEntity ce:values) {
				this.secondPart=new Text(ce.getSecondPart().toString());
				//左表
				if(ce.getFlag().toString().trim().equals("0")){
					leftTable.add(secondPart);
				}else if(ce.getFlag().toString().trim().equals("1")){
					rightTable.add(secondPart);
				}
			}
			for(Text left:leftTable){

				for(Text right:rightTable){

					output.set(left+"\t"+right);//连接左右数据
					context.write(key, output);//输出
				}
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//JobConf conf = new JobConf(ReduceJoin.class);
		//conf.set(name, value);
		Job job = new Job();
		job.setJarByClass(ReduceJoin.class);

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CombineEntity.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.50:9000/mdata/2215.sta.gz"),new Path("hdfs://192.168.1.50:9000/mdata/info.txt"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReduceJoin(), args);
		System.exit(exitCode);		
	}
}
