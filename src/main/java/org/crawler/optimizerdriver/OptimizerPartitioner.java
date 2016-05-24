package org.crawler.optimizerdriver;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OptimizerPartitioner extends Partitioner<Text,BooleanWritable> {

	@Override
	public int getPartition(Text key, BooleanWritable arg1, int numReduceTasks) {
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
