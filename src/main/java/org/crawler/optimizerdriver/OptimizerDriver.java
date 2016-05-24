package org.crawler.optimizerdriver;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerDriver extends Configured implements Tool {

	
	private static Configuration conf =  HBaseConfiguration.create();
	
	static{
		conf.addResource("app-config.xml");
	}
	
	public static class OptimizerMapper extends
			TableMapper<Text, BooleanWritable> {

		public void map(ImmutableBytesWritable key, Result values,
				Context context) throws IOException, InterruptedException {
			BooleanWritable trueflag = new BooleanWritable(true);
			BooleanWritable falseflag = new BooleanWritable(false);

			for (Cell cell : values.listCells()) {
				String direct = Bytes.toString(CellUtil.cloneRow(cell));
				context.write(new Text(direct), trueflag);
				if ("outLinks".equals(Bytes.toString(CellUtil.cloneQualifier(cell))) && Bytes.toString(CellUtil.cloneValue(cell)) != "") {
					String outLinks = new String(CellUtil.cloneValue(cell),"gb2312");
					System.out.println("outLinks:" + outLinks);
					String[] links = outLinks.split(",");
					for (String url : links) {
						Text urltext = new Text(url);
						System.out.println("url:" + url + ",flag:"
								+ falseflag.toString());
						context.write(urltext, falseflag);
					}
				}
			}
		}
	}

	public static class OptimizerReducer extends
			TableReducer<Text, BooleanWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<BooleanWritable> value,
				Context context) throws IOException, InterruptedException {

			Iterator<BooleanWritable> v = value.iterator();
			boolean flag = false;
			while (v.hasNext()) {
				BooleanWritable booleanWritable = v.next();
				if (booleanWritable.get() == true) {
					// System.out.println("value:"+booleanWritable.get());
					flag = true;
					break;
				}
			}
			System.out.println("key" + key.toString());
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("in"), Bytes.toBytes("flag"),
					Bytes.toBytes(flag));
			context.write(
					new ImmutableBytesWritable(Bytes.toBytes(key.toString())),
					put);

		}
	}

	public int run(String[] args) throws Exception {

		String tablename = conf.get("tablename");
		
		Job job = Job.getInstance(conf, "OptimizerDriver");
		job.setJarByClass(OptimizerDriver.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tablename);

		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setInputFormatClass(TableInputFormat.class);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("url"));
		TableMapReduceUtil.initTableMapperJob(tablename, scan,
				OptimizerMapper.class, Text.class, BooleanWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(tablename,
				OptimizerReducer.class, job);

		job.waitForCompletion(true);
		return job.isSuccessful() ? 1 : 0;
	}

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(OptimizerDriver.class);
		try {
			int returnCode = ToolRunner.run(new OptimizerDriver(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
