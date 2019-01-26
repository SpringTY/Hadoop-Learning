package com.spring.mr.flow;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FLowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] info = StringUtils.split(line, "\t");
		context.write(new Text(info[1]),
				new FlowBean(info[1], Long.parseLong(info[info.length - 3]), Long.parseLong(info[info.length - 2])));

	}
}
