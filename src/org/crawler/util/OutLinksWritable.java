package org.crawler.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class OutLinksWritable implements WritableComparable<OutLinksWritable> {

	/**
	 * 一个存放 url 的数组。
	 * 如果未发生重定向则 outlinks为从网页中解析出来的链出链接集合；
	 * 否则,outlinks只有一个元素，为重定向后的真实 url
	 */
	private TextArrayWritable outLinks = new TextArrayWritable();
	
	/**
	 * 标记 outlinks 数组里的 url 类型。
	 * 如果是链出链接集合则 typeOfOutlink 为0；
	 * 如果是真实 url 则 typeOfOutlink 为1。
	 */
	private IntWritable typeOfOutlink;
	
	/**
	 * 每当链出 URL 库中写入一个 OutLinksWritable 类型的数据时
	 * 会为这个数据生成一个时间戳，用来区分不同的数据
	 */
	private LongWritable timeStamp;
	

	public TextArrayWritable getOutLinks() {
		return outLinks;
	}

	public void setOutLinks(TextArrayWritable outLinks) {
		this.outLinks = outLinks;
	}

	public IntWritable getTypeOfOutlink() {
		return typeOfOutlink;
	}

	public void setTypeOfOutlink(IntWritable typeOfOutlink) {
		this.typeOfOutlink = typeOfOutlink;
	}

	public LongWritable getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(LongWritable timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.outLinks.readFields(input);
		this.timeStamp.readFields(input);
		this.typeOfOutlink.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		this.outLinks.write(output);
		this.timeStamp.write(output);
		this.typeOfOutlink.write(output);
	}

	@Override
	public int compareTo(OutLinksWritable arg0) {
		// TODO Auto-generated method stub
		int cmp = this.outLinks.toString().compareTo(arg0.outLinks.toString());
		if(cmp != 0){
			return cmp;
		}
		int cmp2 = this.timeStamp.compareTo(arg0.timeStamp);
		if(cmp != 0){
			return cmp2;
		}
		return this.typeOfOutlink.compareTo(arg0.typeOfOutlink);
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		String[] urls = this.outLinks.toStrings();
		int i = 1;
		for(String t : urls){
			
			System.out.println("url"+i+": "+t);
			str.append(t).append("|");
			i++;
		}
		str.append(this.typeOfOutlink+"|"+this.timeStamp);
		return str.toString();
	}
	

}
