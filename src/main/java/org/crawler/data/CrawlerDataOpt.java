/**
 * 
 */
package org.crawler.data;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author lin
 *
 */
public class CrawlerDataOpt {

	private static Configuration conf = HBaseConfiguration.create();
	private static final String TABLENAME = "crawlertest";
	
	private static HTable htable = null;
	private static String columnfamily = "xml";
	
	public static HTable getHTable() throws IOException{
		if(htable != null){
			return htable;
		}else{
			htable = new HTable(conf,TABLENAME);
			return htable;
		}
	}
	
	public void scan(){
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		HTable htable = getHTable();
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnfamily));
		
		ResultScanner resultScanner = htable.getScanner(scan);
		
		Iterator<Result> results = resultScanner.iterator();
		
	
		while(results.hasNext()){
			Result result = results.next();
			for(Cell cell : result.listCells()){
				String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println("KEY:"+rowkey+"\n  VALUE: \n"+value);
			}
		}
	}

}
