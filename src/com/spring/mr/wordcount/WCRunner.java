package com.spring.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用来描述一个特性的作业，指定map和reduce
 * 可以指定该作业要处理的数据所在路径
 * 可以指定该作业结果路径
 * @author spring
 * com.spring.mr.wordcount.WCRunner
 */
public class WCRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 设置job所在类所在jar包
		job.setJarByClass(WCRunner.class);
			
		// 指定job使用mapper和reduce
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		// 指定输出数据k-v类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// 指定mapper输出的k-v类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// 指定输入数据路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://spring:9000/wc/srcdata"));
		// 处理输出结果的存放路径
		FileOutputFormat.setOutputPath(job, new Path("/Users/spring/output"));
		
		//将job提交给集群运行
		job.waitForCompletion(false);
	}

}
