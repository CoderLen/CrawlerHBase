package org.crawler.htmltoxmldriver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HtmlToXMLDriver  extends Configured implements Tool{
	
	private static Configuration conf =  HBaseConfiguration.create();
	
	static{
		conf.addResource("app-config.xml");
	}
	
	public static class HtmlToXMLMapper extends TableMapper<ImmutableBytesWritable,Put>{

		public void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{

			for(Cell cell : value.rawCells()){
				if("document".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					String url  =  Bytes.toString(CellUtil.cloneRow(cell));
					String document = Bytes.toString(CellUtil.cloneValue(cell));
					String xml = htmltoxml(url,document);
					Put put = new Put(Bytes.toBytes(url));
					put.add(Bytes.toBytes("xml"), Bytes.toBytes("info"), Bytes.toBytes(xml));
					context.write(key, put);
				}
			}

		}
		
		public static String htmltoxml(String url,String document) throws UnsupportedEncodingException{
			
			String title = "";
			String content = "";
			
			String regex1= "<title>.*?</title>";
			String regex2 = "\"articleBody\">.*?</div>";
			Pattern pattern1= Pattern.compile(regex1);
			Pattern pattern2 = Pattern.compile(regex2);
			Matcher mt1 = pattern1.matcher(document);
			Matcher mt2 = pattern2.matcher(document);
		    if(mt1.find()){
		    	title = mt1.group().replaceAll("<.*?>",	 "");
		     	System.out.println("title:"+title);
		    }
		    if(mt2.find()){
		    	content = mt2.group().replaceAll("<.*?>",	 "");
		    	System.out.println("html:"+content);
		    }
			StringBuffer xml = new StringBuffer();
			xml.append("<doc>"+"\n");
			xml.append("<url>"+url +"</url>"+"\n");
			xml.append("<title>"+title+"</title>"+"\n");
			xml.append("<content>" + content + "</content>"+"\n");
			xml.append("</doc>");
			return  xml.toString();
		}
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		String tablename = conf.get("tablename");
		
		Job job = Job.getInstance(conf, "HtmlToXMLDriver");
		job.setJarByClass(HtmlToXMLDriver.class);
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tablename);  
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setInputFormatClass(TableInputFormat.class);
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("doc"));
		TableMapReduceUtil.initTableMapperJob(tablename, scan,HtmlToXMLMapper.class , ImmutableBytesWritable.class, Put.class, job);
		
		job.waitForCompletion(true);
		return job.isSuccessful() ? 1 : 0;
	}
	
	public static void main(String[] args){
		 Logger logger = LoggerFactory.getLogger(HtmlToXMLDriver.class);
		try {
			int returnCode = ToolRunner.run(new HtmlToXMLDriver(),args);
			System.exit(returnCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
