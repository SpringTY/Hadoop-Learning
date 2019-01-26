package com.spring.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author spring
 * 输入 词（String）和1（Long）
 * 输出 词（String）和词频（LongWritable）
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	// 框架在map处理完成以后，将所有的kv对缓存起来，进行分组，然后传递一个组<key,values{}>，调用一次reduce方法
	// just like : <hello,{1,1,1,1,1,1}>
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable longValues : values) {
			count += longValues.get();
		}
		context.write(key, new LongWritable(count));
		
	}
}
