package com.spring.mr.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortMR extends Configured implements Tool{
	private static class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, SortBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] info = StringUtils.split(line, "\t");
			context.write(new SortBean(info[0], Long.parseLong(info[1]), Long.parseLong(info[2])), NullWritable.get());
		}
	}

	private static class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
		@Override
		protected void reduce(SortBean key, Iterable<NullWritable> values,
				Reducer<SortBean, NullWritable, SortBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new SortMR(), args);
		System.exit(status);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SortMR.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setOutputKeyClass(SortBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(SortBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/spring/hadoopSpace/sort/data/"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/spring/hadoopSpace/sort/output/"));
		return job.waitForCompletion(true)?0:1;
	}

}
