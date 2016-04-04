package com.epam.bdc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zenind
 */
public class IPinYouJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Sort IPinYou");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IPinYouMapper.class);
        job.setReducerClass(IPinYouReducer.class);

        job.setOutputKeyClass(IPinYouWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(IPinYouPartitioner.class);
        job.setGroupingComparatorClass(IPinYouGroupComparator.class);

        final int result = job.waitForCompletion(true) ? 0 : 1;
        Counters counters = job.getCounters();
        System.out.println(counters);

        return result;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IPinYouJob(), args));
    }
}
