package com.epam.bdc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.UUID;

/**
 * @author zenind
 */
public class IPinYouReducer extends Reducer<IPinYouWritable, Text, NullWritable, Text> {

    private final UUID rId = UUID.randomUUID();

    private int maxSiteImpressionCount = 0;

    private IPinYouWritable maxSiteImpressionIPinYouId;

    @Override
    protected void reduce(IPinYouWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text value : values) {
            final String[] itemValues = value.toString().split("\t");
            if (StreamType.SITE_IMPRESSION.getType().equalsIgnoreCase(itemValues[21])) {
                count++;
            }
            context.write(NullWritable.get(), value);
        }

        if (count > maxSiteImpressionCount) {
            maxSiteImpressionCount = count;
            maxSiteImpressionIPinYouId = key;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (maxSiteImpressionIPinYouId != null) {
            context.getCounter(StreamType.SITE_IMPRESSION.name(), maxSiteImpressionIPinYouId.getId().toString())
                .setValue(maxSiteImpressionCount);
        }
    }
}
