package sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class Mr_10 extends Configured implements Tool {

	public static class SogouMapper1 extends Mapper<Object, Text, Text, NullWritable> {
		private Text uid = new Text();
		private NullWritable tmp = null;
		
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t", -1);
			if (arr.length == 6) {
				if (arr[2].indexOf("仙剑奇侠传") != -1) {
					uid.set(arr[1]);
					context.write(uid, tmp);					
				}
			}
			
		}
	}
	
	public static class SogouReducer1 extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
 
	public static void main(String[] args) {
		
	}
}
