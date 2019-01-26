package com.spring.mr.parition;

import java.util.Hashtable;

import org.apache.hadoop.mapreduce.Partitioner;

// partitioner 的input k-v 是map输出的k-v 也就是reduce 输入的k-v 类型 
public class AreaPartitioner<KEY,VALUE> extends Partitioner<KEY, VALUE>
{
	private static Hashtable<String, Integer> areaMap = new Hashtable<>();
	static {
		// load database
		areaMap.put("135", 1);
		areaMap.put("136", 2);
		areaMap.put("137", 3);
		areaMap.put("138", 4);
		areaMap.put("139", 0);
	}
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		//从Key中拿到手机号，查询手机归属地字典
		Integer res = areaMap.get(key.toString().substring(0,3));
		return res==null?5:res;
	}

}
