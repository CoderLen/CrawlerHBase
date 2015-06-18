package org.crawler.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//�ӿ�Partitioner�̳�JobConfigurable����������������override����
public class HostShuffle extends Partitioner<Text, LongWritable>{

	/** 
     * getPartition()������ 
     * �����������/ֵ��<key,value>��reducer����numReduceTasks 
     * ��������������Reducer��ţ�������result 
     * */  
	@Override
	public int getPartition(Text key, LongWritable value, int numReduceTasks) {
		 // TODO Auto-generated method stub
		String url = key.toString();
		if(url.contains("http")){
			url = url.substring(url.indexOf("//"), url.indexOf("/", url.indexOf("//")));
		}else if(url.contains("/")){
			url = url.substring(0, url.indexOf("/"));
		}
		return (url.hashCode() & Integer.MAX_VALUE) % numReduceTasks;  
	}


}
