package com.epam.bdc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zenind
 */
public class IPinYouReducer extends Reducer<IPinYouWritable, Text, NullWritable, Text> {

    private enum StreamType {
        SITE_IMPRESSION("1"),
        SITE_CLICK("11");

        private final String type;

        StreamType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    @Override
    protected void reduce(IPinYouWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (Text value : values) {
            final String[] itemValues = value.toString().split("\t");
            if (StreamType.SITE_IMPRESSION.getType().equalsIgnoreCase(itemValues[21])) {
                count++;
            }
            context.write(NullWritable.get(), value);
        }

        context.getCounter(StreamType.SITE_IMPRESSION.name(), key.getId().toString()).setValue(count);
    }

}
