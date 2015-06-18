package org.crawler.util;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class OptimizerRecordReader extends RecordReader<Text,OutLinksWritable>{

	private LineReader in;
	private boolean more = false;
	
	private Text key = null;
	private OutLinksWritable value = null;
	
	private long start;
	private long pos;
	private long end;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(null != in){
			in.close();
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public OutLinksWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		if(false == more || end == start){
			return 0f;
		}else{
			return Math.min(1.0f, (pos - start)/(end - start));
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fileSplit = (FileSplit)split;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());
		
		fileIn.seek(start);
		
		in = new LineReader(fileIn,context.getConfiguration());
		if(start != 0){
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
	}

	private int maxBytesToConsume(long pos){
		return (int)Math.min(Integer.MAX_VALUE, end - pos);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(null == key){
			key = new Text();
		}
		if(null == value){
			value = new OutLinksWritable();
		}
		
		Text nowline = new Text();
		int readsize = in.readLine(nowline);
		//更新当前读取到的位置
		pos += readsize;
		//如果pos值大于等于end，说明此分片已经读取完毕
		if(pos >= end){
			more = false;
			return false;
		}
		
		if(0 == readsize){
			key = null;
			value = null;
			more = false;//此处说明已经读取到文件末尾
			return false;
		}
		
		String[] keyandvalue = nowline.toString().split("\t");
		//排除第一行
		if(keyandvalue[0].endsWith("\"CITING\"")){
			readsize = in.readLine(nowline);
			//更新当前读取到位置
			pos += readsize;
			if(0 == readsize){
				more = false;
				return false;
			}
			//重新划分
			keyandvalue = nowline.toString().split(",");
		}
		
		key.set(keyandvalue[0]);
		String[] values = keyandvalue[1].toString().split("|");
		
		if(values.length > 3){
			Text[] urltext = new Text[values.length - 2];
			for(int i=0;i<values.length - 2;i++){
				urltext[i] = new Text(values[i]);
			}
			LongWritable timestramp = new LongWritable(Long.parseLong(values[values.length-1]));
			IntWritable typeOfOutLink = new IntWritable(Integer.parseInt(values[values.length-2]));
			//timestramp.set(Long.parseLong(values[values.length-2]));
			TextArrayWritable urls = new TextArrayWritable(urltext);
			value.setOutLinks(urls);
			value.setTimeStamp(timestramp);
			value.setTypeOfOutlink(typeOfOutLink);
		}
		
		return true;
	}

}
