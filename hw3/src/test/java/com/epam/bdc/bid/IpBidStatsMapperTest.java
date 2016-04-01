package com.epam.bdc.bid;

import com.beust.jcommander.internal.Lists;
import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author zenind
 */
public class IpBidStatsMapperTest {

    @Test(dataProvider = "bid-mapper-data")
    public void testBidStatsParse(String input, Pair<Text, IpBidStatsWritable> output) throws Exception {
        new MapDriver<LongWritable, Text, Text, IpBidStatsWritable>()
            .withMapper(new IpBidStatsMapper())
            .withInput(new LongWritable(0), new Text(input))
            .withOutput(output)
            .runTest();
    }

    @DataProvider(name = "bid-mapper-data")
    public Object[][] bidMapperData() {
        return new Object[][]{
            {
                "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0",
                new Pair<>(new Text("180.127.189.*"), new IpBidStatsWritable(1, 227L))
            },
            {
                "f5f0f6179b5bf7ab9aecbf1197766a2f\t20130606000104000\tnull\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t163.179.15.*\t216\t234\t3\t5F1RQS9rg5scFsf\t2894177023e3411e2dbb0e9b5186c2cb\tnull\tNews_Pic_Width2\t960\t90\t0\t0\t50\tfb5afa9dba1274beaf3dad86baf97e89\t300\t1458\t282163094838\t0",
                new Pair<>(new Text("163.179.15.*"), new IpBidStatsWritable(1, 300L))
            }
        };
    }

    @Test
    public void testBrowserStatsCounters() throws Exception {
        Counters counters = new Counters();
        CounterGroup browserGroup = counters.addGroup(ProcessEvent.BROWSER_INFO.name(), ProcessEvent.BROWSER_INFO.name());
        browserGroup.addCounter(Browser.IE6.name(), Browser.IE6.name(), 2L);
        browserGroup.addCounter(Browser.FIREFOX21.name(), Browser.FIREFOX21.name(), 1L);
        MapDriver driver = new MapDriver<LongWritable, Text, Text, IpBidStatsWritable>()
            .withMapper(new IpBidStatsMapper())
            .withInput(new LongWritable(0), new Text("b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0"))
            .withInput(new LongWritable(1), new Text("b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0"))
            .withInput(new LongWritable(2), new Text("78157745d4bfad7c71ec7dd4add1152a\t20130606000104000\tVh1iL5xAPlq_Q-a\tMozilla/5.0 (Windows NT 6.0; rv:21.0) Gecko/20100101 Firefox/21.0,gzip(gfe),gzip(gfe)\t171.212.96.*\t276\t277\t2\ttrqRTukYQ9uRJmqf5SqfNX\tb17fc8edf1a130c9649182b54eaadd27\t\t521759013\t728\t90\t1\t0\t5\t4ad7e35171a3d8de73bb862791575f2e\t238\t3358\t282825712806\t0"))
            .withAllOutput(Lists.newArrayList(
                new Pair<>(new Text("180.127.189.*"), new IpBidStatsWritable(1, 227L)),
                new Pair<>(new Text("180.127.189.*"), new IpBidStatsWritable(1, 227L)),
                new Pair<>(new Text("171.212.96.*"), new IpBidStatsWritable(1, 238L))
            ));
        driver.runTest();
        Assert.assertEquals(driver.getCounters(), counters);
    }

}
