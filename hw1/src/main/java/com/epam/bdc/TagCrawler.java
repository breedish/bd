package com.epam.bdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

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
        crawler.run();
    }

    public void run() {
        System.out.println("Running Crawler for " + seed + " urls seed and output " +  output);
        System.out.println(configuration);
    }
}
