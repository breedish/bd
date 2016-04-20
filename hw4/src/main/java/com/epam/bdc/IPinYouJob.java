package com.epam.bdc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.IntSummaryStatistics;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.reducing;

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

        String maxImpressionIPinYouId = null;
        long maxCount = 0;
        for (Counter counter : counters.getGroup(StreamType.SITE_IMPRESSION.name())) {
            if (counter.getValue() > maxCount) {
                maxCount = counter.getValue();
                maxImpressionIPinYouId = counter.getName();
            }
        }

        System.out.println("Max IPinYou site impression id = " + maxImpressionIPinYouId);

        return result;
    }

    public static int doSeq(int n) {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            sum += new Integer(i);
        }
        return sum;
    }

    public static int doSeq2(int n) {
        return Stream.iterate(0, (Integer i) -> i + 1)
            .limit(n)
            .reduce((i, j) -> i + j).get();
    }

    public static int doPar(int n) {
        return IntStream.rangeClosed(1, n)
            .boxed()
            .reduce(0, (i, j) -> i + j);
    }

    public static int doPar2(int n) {
        return IntStream.rangeClosed(1, n)
            .parallel()
            .reduce(0, Integer::sum);
    }

    public static void main(String[] args) throws Exception {
//        System.exit(ToolRunner.run(new IPinYouJob(), args));

        int result = IntStream.rangeClosed(1, 3)
            .boxed()
            .collect(reducing(0, (i) -> i, (i, j) -> i + j / 2));
        System.out.println(result);

        IntSummaryStatistics stats = IntStream.rangeClosed(1, 100)
            .boxed()
            .collect(Collectors.summarizingInt(i -> i));

        System.out.println(stats);

        int n = 10000000;

        long start = System.currentTimeMillis();
        doSeq(n);
        System.out.println("Seq = " + (System.currentTimeMillis() - start)/1000.0);

        start = System.currentTimeMillis();
        doSeq2(n);
        System.out.println("Seq2 = " + (System.currentTimeMillis() - start)/1000.0);

        start = System.currentTimeMillis();
        doPar(n);
        System.out.println("Par = " + (System.currentTimeMillis() - start)/1000.0);

        start = System.currentTimeMillis();
        doPar2(n);
        System.out.println("Par2 = " + (System.currentTimeMillis() - start)/1000.0);



//        Stream.iterate(new int[]{0, 1}, (int[] p) -> new int[]{p[1], p[0] + p[1]})
//            .limit(10)
//            .forEach((int[] p) -> System.out.print("(" + p[0] + "," + p[1] + ")"));
    }
}
