package com.epam.bdc.tag;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zenind
 */
public class CountTagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        values.forEach(o -> count.addAndGet(o.get()));
        context.write(key, new IntWritable(count.get()));
    }

}
