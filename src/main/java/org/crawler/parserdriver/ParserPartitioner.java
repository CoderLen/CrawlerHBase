package org.crawler.parserdriver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.crawler.util.DocumentWritable;

public class ParserPartitioner extends Partitioner<Text, DocumentWritable> {

    @Override
    public int getPartition(Text key, DocumentWritable arg1, int numReduceTasks) {
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
