package com.spring.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
// 4个范型中，前两个是指定mapper输入数据的类型KEYIN是输入key的类型 VALUEIN是输入的value的类型
// map 和 reduce 数据的输入和输出都是key-value对的形式封装
// 默认情况下，框架传递给我们的mapper的输入数据中，key是要处理文本中一行的起始偏移量，内容作为value
// 输出 词(String) + 1(Long)
/*
    Long 和 String 序列化信息太多，Hadoop 实现了精简化的类型
 	Long -> LongWritable 
 	String -> Text
 * */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	// mapreduce 每读一行数据就调用一次这个方法
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// 具体业务逻辑写在这里，在方法的参数中key-value
		// key 是这一行的起始偏移量
		// value 是这一行的文本内容
		// context 输出
		
		// get line String
		String line = value.toString();
		// split lines
		String[] words = StringUtils.split(line," ");
		// traverse the array to out like "v" : 1
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
