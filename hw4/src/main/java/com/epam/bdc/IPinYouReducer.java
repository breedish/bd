package com.epam.bdc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zenind
 */
public class IPinYouReducer extends Reducer<IPinYouWritable, Text, IPinYouWritable, Text> {

    @Override
    protected void reduce(IPinYouWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
