package org.crawler.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class OptimizerInputFormat extends FileInputFormat<Text, OutLinksWritable> {

    @Override
    public RecordReader<Text, OutLinksWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new OptimizerRecordReader();
    }

}
