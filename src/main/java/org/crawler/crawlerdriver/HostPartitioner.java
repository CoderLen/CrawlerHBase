package org.crawler.crawlerdriver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//接口Partitioner继承JobConfigurable，所以这里有两个override方法
public class HostPartitioner extends Partitioner<Text, LongWritable> {


    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        String url = key.toString();
        if (url.contains("http://")) {
            url = url.replace("http://", "");
        }
        if (url.contains("/")) {
            url = url.substring(0, url.indexOf("/"));
        }
        return (url.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }


}
