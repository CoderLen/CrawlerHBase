package org.crawler.crawlerdriver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//接口Partitioner继承JobConfigurable，所以这里有两个override方法
public class HostPartitioner extends Partitioner<Text, LongWritable>{

	/** 
     * getPartition()方法的 
     * 输入参数：键/值对<key,value>与reducer数量numReduceTasks 
     * 输出参数：分配的Reducer编号，这里是result 
     * */  
	@Override
	public int getPartition(Text key, LongWritable value, int numReduceTasks) {
		 // TODO Auto-generated method stub
		String url = key.toString();
		if(url.contains("http://")){
			url = url.replace("http://", "");
		}
		if(url.contains("/")){
			url = url.substring(0, url.indexOf("/"));
		}
		return (url.hashCode() & Integer.MAX_VALUE) % numReduceTasks;  
	}


}
