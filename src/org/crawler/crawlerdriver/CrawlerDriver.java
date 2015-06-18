/**
 * 
 */
package org.crawler.crawlerdriver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.crawler.util.DocumentWritable;
import org.crawler.util.Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lin
 *
 */
public class CrawlerDriver extends Configured implements Tool {

	public static Logger logger = LoggerFactory.getLogger(CrawlerDriver.class);
	
	private static Configuration conf =  HBaseConfiguration.create();
	
	static{
		conf.addResource("app-config.xml");
	}
	
	public static class InverseMapper extends TableMapper<Text, LongWritable> {

		private Text key = new Text();
		private LongWritable value = new LongWritable(1);

		public void map(ImmutableBytesWritable ikey, Result values,
				Context context) throws IOException, InterruptedException {

			for (Cell cell : values.rawCells()) {
				boolean flag = Boolean.parseBoolean(Bytes.toString(CellUtil
						.cloneValue(cell)));
				if (flag == false) {
					key.set(CellUtil.cloneRow(cell));
					context.write(key, value);
				}
			}
		}

	}

	public static class CrawlerReducer extends
			TableReducer<Text, LongWritable, ImmutableBytesWritable> {
		public void reduce(Text url, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println("Start CrawlerReducer...");
			System.out.println("url:" + url);
			if (url != null && url.toString() != "") {
				String document = Downloader.Download(url);
				if (null != document) {
					Put put = new Put(Bytes.toBytes(url.toString()));
					DocumentWritable documentWritable = new DocumentWritable(
							url.toString(), document);
					put.add(Bytes.toBytes("doc"), Bytes.toBytes("document"),Bytes.toBytes(documentWritable.getDocument()));
					put.add(Bytes.toBytes("doc"),
							Bytes.toBytes("redirectFrom"),
							Bytes.toBytes(documentWritable.getRedirectFrom()));
					put.add(Bytes.toBytes("doc"), Bytes.toBytes("metaFollow"),
							Bytes.toBytes(documentWritable.getMetaFollow()));
					put.add(Bytes.toBytes("doc"), Bytes.toBytes("metaIndex"),
							Bytes.toBytes(documentWritable.getMetaIndex()));
					context.write(
							new ImmutableBytesWritable(Bytes.toBytes(url
									.toString())), put);
				}
			}

		}
	}

	public int run(String[] args) throws ClassNotFoundException, IOException,
			InterruptedException {
		
		String tablename = conf.get("tablename");
		Job job = Job.getInstance(conf, "CrawlerDriver");
		job.setJarByClass(CrawlerDriver.class);

		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// TODO: specify output types
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("in"));
		scan.setCaching(10);
		scan.setBatch(10);
		TableMapReduceUtil.initTableMapperJob(tablename, scan,
				InverseMapper.class, Text.class, LongWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tablename, CrawlerReducer.class,
				job);

		job.waitForCompletion(true);
		return job.isSuccessful() ? 1 : 0;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Logger logger = LoggerFactory.getLogger(CrawlerDriver.class);
		try {
			int returnCode = ToolRunner.run(new CrawlerDriver(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}

}
