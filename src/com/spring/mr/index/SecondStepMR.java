package com.spring.mr.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.io.Files;
/**
 * 
 * @author spring
 * 可以改进策略: 定义 wordBean 
 * 属性：单词 次数	来源
 * 第一步：生成
 * word source count
 * 第二步: 生成
 * word wordBeans(sorted)		
 */

public class SecondStepMR extends Configured implements Tool {
	private static Path in = null;
	private static Path out = null;
	// word-->essay count
	private static class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String word = split[0].split("-->")[0];
			String count = split[1];
			String essay = split[0].split("-->")[1];
			context.write(new Text(word), new Text(essay+"-"+count));
		}
	}
	private static class SecondReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer stringb = new StringBuffer();
			for (Text text : values) {
				stringb.append(text.toString()+" ");
			}
			context.write(key, new Text(stringb.toString()));			
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job secondJob = Job.getInstance();
		
		secondJob.setJarByClass(SecondStepMR.class);
		
		secondJob.setReducerClass(SecondReducer.class);
		secondJob.setMapperClass(SecondMapper.class);
		
		secondJob.setMapOutputKeyClass(Text.class);
		secondJob.setMapOutputValueClass(Text.class);
		
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);
		
		in = new Path(args[0]);
		out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out,true);
		}
		FileInputFormat.setInputPaths(secondJob, in);
		FileOutputFormat.setOutputPath(secondJob, out);
		return secondJob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
//		args = new String[2];
//		args[0] = "/Users/spring/hadoopSpace/search/out1";
//		args[1] = "/Users/spring/hadoopSpace/search/outfinal";
		int res = ToolRunner.run(new SecondStepMR(), args);
		System.exit(res);
	}
}
