package com.epam.bdc.tag;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author zenind
 */
public final class CountTagMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<String, List<String>> tags = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] cacheFiles = context.getCacheFiles();
        try {
            if (cacheFiles.length > 0) {
                try (Stream<String> logEntries = Files.lines(Paths.get(new File(cacheFiles[0].getPath()).getName()))) {
                    logEntries.forEach(e -> {
                        String[] values = e.split("\t");
                        if (values.length > 0) {
                            tags.put(values[0], Splitter.on(",").splitToList(values[1]));
                        }
                    });
                }
            }
        } catch (Exception e) {
            System.err.println(Throwables.getStackTraceAsString(e));
        }
    }

    @Override
    protected void map(LongWritable key, Text inValue, Context context) throws IOException, InterruptedException {
        String value = inValue.toString();
        if (!Strings.isNullOrEmpty(value)) {
            String[] values = value.split("\t");
            if (values.length == 22) {
                String linkId = values[20];
                for (String tag : tags.getOrDefault(linkId, Collections.emptyList())) {
                    context.write(new Text(tag), new IntWritable(1));
                }
            }
        }
    }
}
