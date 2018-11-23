/**
 *
 */
package org.crawler.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author lin
 */
@Slf4j
public class CrawlerDataOpt {

    private static final String TABLENAME = "crawlertest";
    private static Table htable = null;
    private static String columnfamily = "xml";

    public static Table getHTable() throws IOException {
        if (htable != null) {
            return htable;
        } else {
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource("hdfs-site.xml");
            configuration.addResource("core-site.xml");
            configuration.addResource("mapred-site.xml");
            Connection connection = ConnectionFactory.createConnection(configuration);
            htable = connection.getTable(TableName.valueOf(TABLENAME));
            return htable;
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Table htable = getHTable();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnfamily));
        ResultScanner resultScanner = htable.getScanner(scan);
        Iterator<Result> results = resultScanner.iterator();

        while (results.hasNext()) {
            Result result = results.next();
            for (Cell cell : result.listCells()) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                log.info("KEY: {}, VALUE: {}", rowkey, value);
            }
        }
    }


}
