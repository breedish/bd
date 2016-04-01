package com.epam.bdc.bid;

import com.google.common.base.Strings;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zenind
 */
public class IpBidStatsMapper extends Mapper<LongWritable, Text, Text, IpBidStatsWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String bidEntry = value.toString();
        if (!Strings.isNullOrEmpty(bidEntry)) {
            String[] values = bidEntry.split("\t");
            if (values.length == 22) {
                try {
                    trackUserAgent(context, values);
                    context.write(new Text(values[4]), new IpBidStatsWritable(1, Long.parseLong(values[18])));
                } catch (Exception e) {
                    context.getCounter(ProcessEvent.BROKEN_DATA).increment(1);
                }
            }
        }
    }

    private void trackUserAgent(Context context, String[] values) {
        UserAgent userAgent = new UserAgent(values[3]);
        context.getCounter(ProcessEvent.BROWSER_INFO.name(), userAgent.getBrowser().name()).increment(1);
    }
}
