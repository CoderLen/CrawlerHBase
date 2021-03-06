/**
 *
 */
package org.crawler.crawlerdriver;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.crawler.util.DocumentWritable;
import org.crawler.util.Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author lin
 */
@Slf4j
public class CrawlerDriverFromLocal {


    public static Logger logger = LoggerFactory.getLogger(CrawlerDriverFromLocal.class);

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */

    public static void main(String[] args) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String tablename = "crawler";

        Configuration conf = HBaseConfiguration.create();

        Job job = Job.getInstance(conf, "CrawlerDriver");
        job.setJarByClass(CrawlerDriverFromLocal.class);

        job.setMapperClass(InverseMapper.class);

        job.setPartitionerClass(HostPartitioner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        TableMapReduceUtil.initTableReducerJob(tablename, CrawlerReducer.class, job);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/in"));

        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static class InverseMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            logger.info("InverseMapper.....");
            log.info("key: " + ikey);
            log.info("value:" + ivalue);
            logger.info("InverseMapper End!");
            context.write(ivalue, ikey);
        }

    }

    public static class CrawlerReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {

        @Override
        public void reduce(Text url, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            log.info("Start CrawlerReducer...");
            logger.info("url:" + url);
            log.info("url:" + url);
            if (url != null && url.toString() != "") {
                if (url.toString().contains("http://")) {
                    String document = Downloader.Download(url);
                    log.info("url:" + url);
                    log.info("document:" + document);
                    if (null != document) {
                        Put put = new Put(Bytes.toBytes(url.toString()));
                        DocumentWritable documentWritable = new DocumentWritable(url.toString(), document);
                        put.addColumn(Bytes.toBytes("doc"), Bytes.toBytes("document"), Bytes.toBytes(documentWritable.getDocument()));
                        put.addColumn(Bytes.toBytes("doc"), Bytes.toBytes("redirectFrom"), Bytes.toBytes(documentWritable.getRedirectFrom()));
                        put.addColumn(Bytes.toBytes("doc"), Bytes.toBytes("metaFollow"), Bytes.toBytes(documentWritable.getMetaFollow()));
                        put.addColumn(Bytes.toBytes("doc"), Bytes.toBytes("metaIndex"), Bytes.toBytes(documentWritable.getMetaIndex()));
                        context.write(new ImmutableBytesWritable(Bytes.toBytes(url.toString())), put);
                    }
                }
            }

        }
    }


}
