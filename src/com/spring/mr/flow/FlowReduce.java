package com.spring.mr.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean>{
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		Long up_flows = (long) 0;
		Long down_flows = (long)0;
		for (FlowBean flowBean : values) {
			up_flows += flowBean.getUp_flow();
			down_flows += flowBean.getDown_flow();
		}
		context.write(key,new FlowBean(key.toString(),up_flows,down_flows));
	}
}
