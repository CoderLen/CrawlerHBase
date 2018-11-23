package org.crawler.parserdriver;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.crawler.util.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;


public class ParserDriver extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(ParserDriver.class);

    private static Configuration conf = HBaseConfiguration.create();

    static {
        conf.addResource("app-config.xml");
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ParserDriver.class);
        try {
            int returnCode = ToolRunner.run(new ParserDriver(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        String tablename = conf.get("tablename");

        Job job = Job.getInstance(conf, "ParserDriver");
        job.setJarByClass(ParserDriver.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.setInputFormatClass(TableInputFormat.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(tablename, scan, ParserMapper.class, ImmutableBytesWritable.class, Put.class, job);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }

    public static class ParserMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        public void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhMMss");
            logger.info("Start ParserMapper...");
            for (Cell cell : values.listCells()) {
                if ("doc".equals(Bytes.toString(CellUtil.cloneFamily(cell))) && "document".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    String str = new String(CellUtil.cloneValue(cell), "gb2312");
                    Set<String> urls = Parser.parser(str);
                    String outlinks = urls.toString();
                    outlinks = outlinks.substring(1, outlinks.length() - 1);
                    int typeOfOutlink = 0;
                    if (urls.size() < 1 || urls == null) {
                        typeOfOutlink = 1;
                    }
                    String redirect = Bytes.toString(CellUtil.cloneRow(cell));
                    Put put = new Put(Bytes.toBytes(redirect.toString()));
                    put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("redirect"), Bytes.toBytes(redirect.toString()));
                    put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("timeStamp"), Bytes.toBytes(format.format(new Date())));
                    put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("outLinks"), Bytes.toBytes(outlinks));
                    put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("typeOfOutlink"), Bytes.toBytes(typeOfOutlink));
                    context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
                }
            }
        }
    }
}
