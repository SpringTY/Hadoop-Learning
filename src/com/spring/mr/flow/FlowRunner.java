package com.spring.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowRunner extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FlowRunner(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME","hadoop");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowRunner.class);
		
		job.setMapperClass(FLowMapper.class);
		job.setReducerClass(FlowReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		FileInputFormat.setInputPaths(job, new Path("/Users/spring/hadoopSpace/flow/data/"));
//		FileOutputFormat.setOutputPath(job, new Path("/Users/spring/hadoopSpace/flow/output/"));
		return job.waitForCompletion(true)?0:1;
	}

}
