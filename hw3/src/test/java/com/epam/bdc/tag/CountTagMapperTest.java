package com.epam.bdc.tag;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

/**
 * @author zenind
 */
public class CountTagMapperTest {

    @Test(dataProvider = "tags-mapper-data", enabled = false)
    public void testProperTagsDetermination(String input, List<Pair<Text, IntWritable>> output) throws Exception {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new CountTagMapper())
            .withInput(new LongWritable(0), new Text(input))
            .withAllOutput(output)
            .withCacheFile(new URI(CountTagMapperTest.class.getResource("/user.profile.tags.us.txt").getPath()))
            .runTest();
    }

    @DataProvider(name = "tags-mapper-data")
    public Object[][] tagsMapperData() {
        return new Object[][]{
            {
                "282163091263\t$16.99,$usd,2016,across,across,all\tON\tCPC\tBROAD\thttp://www.miniinthebox.com/oil-pollution-cleaning-automobile-engine-pipe-with-reinigungspistole-spray-gun-tool_p4815979.html",
                Lists.newArrayList(
                    new Pair<>(new Text("$16.99"), new IntWritable(1)),
                    new Pair<>(new Text("$usd"), new IntWritable(1)),
                    new Pair<>(new Text("2016"), new IntWritable(1)),
                    new Pair<>(new Text("across"), new IntWritable(1)),
                    new Pair<>(new Text("across"), new IntWritable(1)),
                    new Pair<>(new Text("all"), new IntWritable(1))
                )
            }
        };
    }

}
