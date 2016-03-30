package com.epam.bdc;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author zenind
 */
public final class TagCrawler {

    private final Path seed;

    private final Path output;

    private final Configuration configuration;

    public TagCrawler(String urlsSeed, String output) {
        checkArgument(urlsSeed != null && output != null);
        this.seed = new Path(urlsSeed);
        this.output = new Path(output);
        this.configuration = new YarnConfiguration();
    }

    public static void main(String[] args) {
        checkArgument(args.length == 1);
        TagCrawler crawler = new TagCrawler(args[0], args[1]);

        try {
            crawler.run();
        } catch (Exception e) {
            System.err.println(Throwables.getStackTraceAsString(e));
        }
    }

    public void run() throws Exception {
        System.out.println("Running Crawler for " + seed + " urls seed and output " +  output);
        System.out.println(configuration);

        FileSystem fs = FileSystem.get(configuration);
        OutputStream os = fs.create(output);
        InputStream is = new BufferedInputStream(new ByteArrayInputStream("Sample Run".getBytes(StandardCharsets.UTF_8)));
        IOUtils.copyBytes(is, os, configuration);
    }
}
