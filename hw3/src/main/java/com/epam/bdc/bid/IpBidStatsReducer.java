package com.epam.bdc.bid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zenind
 */
public class IpBidStatsReducer extends Reducer<Text, IpBidStatsWritable, Text, IpBidStatsWritable> {

    @Override
    protected void reduce(Text key, Iterable<IpBidStatsWritable> values, Context context) throws IOException, InterruptedException {
        IpBidStatsWritable result = new IpBidStatsWritable();
        values.forEach(s -> {
            result.incCount(s.getCount());
            result.incBid(s.getBid());
        });
        context.write(key, result);
    }
}
