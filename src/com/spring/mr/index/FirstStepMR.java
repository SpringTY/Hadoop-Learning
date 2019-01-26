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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FirstStepMR extends Configured implements Tool {
	private static Path in = null;
	private static Path out = null;
	private static class FirstMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String name = fileSplit.getPath().getName();
			for (String word : words) {
				context.write(new Text(word+"-->"+name), new LongWritable(1));
			}
		}
	}
	private static class FirstReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable longWritable : values) {
				count += 1;
			}
			context.write(key, new LongWritable(count));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job firstJob = Job.getInstance(conf);
		
		firstJob.setJarByClass(FirstStepMR.class);
		
		firstJob.setReducerClass(FirstReducer.class);
		firstJob.setMapperClass(FirstMapper.class);
		
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(LongWritable.class);
		
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(LongWritable.class);
		
		in = new Path(args[0]);
		out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileInputFormat.setInputPaths(firstJob, in);
		FileOutputFormat.setOutputPath(firstJob, out);
		return firstJob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		//debug
//		args = new String [2];
//		args[0] = "/Users/spring/hadoopSpace/search/data";
//		args[1] = "/Users/spring/hadoopSpace/search/out1";
		int res = ToolRunner.run(new FirstStepMR(), args);
		System.exit(res);
	}
}
