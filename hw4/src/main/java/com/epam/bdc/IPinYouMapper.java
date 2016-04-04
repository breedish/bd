package com.epam.bdc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zenind
 */
public class IPinYouMapper extends Mapper<LongWritable, Text, IPinYouWritable, Text> {

    private IPinYouWritable iPinYouWritable = new IPinYouWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        if (isValidEntry(values)) {
            iPinYouWritable.setId(new Text(values[2]));
            iPinYouWritable.setTimestamp(new Text(values[1]));
            context.write(iPinYouWritable, value);
        }
    }

    private boolean isValidEntry(String[] entry) {
        return entry.length == 22 && entry[2] != null && !"null".equalsIgnoreCase(entry[2]);
    }
}
