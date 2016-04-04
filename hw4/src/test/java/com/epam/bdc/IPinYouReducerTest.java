package com.epam.bdc;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author zenind
 */
public class IPinYouReducerTest {

    @Test(dataProvider = "reducerData")
    public void testCorrectCountCalculation(IPinYouWritable inKey, List<Text> inValues, List<Pair<NullWritable, Text>> outValue) throws Exception {
        ReduceDriver driver = new ReduceDriver<IPinYouWritable, Text, NullWritable, Text>()
            .withReducer(new IPinYouReducer())
            .withInput(inKey, inValues)
            .withAllOutput(outValue);
        driver.runTest();

        CounterGroup group = driver.getCounters().getGroup(StreamType.SITE_IMPRESSION.name());
        assertEquals(group.size(), 1);

        Counter counter = group.iterator().next();
        assertEquals(counter.getName(), inKey.getId().toString());
        assertEquals(counter.getValue(), 2L);
    }

    @DataProvider(name = "reducerData")
    public Object[][] reducerData() {
        return new Object[][]{
            {
                new IPinYouWritable(new Text("VhkrPimvL6LftCL"), new Text("20130606000104300")),
                Lists.newArrayList(
                    new Text("a15cd8d54858393cd0df1bd43071b7a4\t20130606000104300\tVhkrPimvL6LftCL\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022),gzip(gfe),gzip(gfe)\t120.86.57.*\t216\t227\t2\teS8-QxdaMNf7gspy\t7feb5935d5a649fe6891181dce21a524\t\t3146654513\t728\t90\t2\t0\t5\t48f2e9ba15708c0146bda5e1dd653caa\t300\t1458\t282162993770\t0"),
                    new Text("586f49e7e088a720449301741e03217e\t20130606000104300\tVhkrPimvL6LftCL\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727),gzip(gfe),gzip(gfe)\t60.222.234.*\t15\t23\t2\tDF9blS9YBquIgqK4wJB\t6120749d193c13bbca36b57540241b31\t\t3261131688\t300\t250\t2\t0\t5\td881a6c788e76c2c27ed1ef04f119544\t238\t3358\t282825712806\t1"),
                    new Text("f2e3a74bc9c17e750ca47a6722c11a95\t20130606000104300\tVhkrPimvL6LftCL\tmozilla/5.0 (ipod; cpu iphone os 6_0 like mac os x) applewebkit/536.26 (khtml, like gecko) mobile/10a406\t123.149.217.*\t164\t165\t1\ttrqRTuu9BqshwMB4JKTI\t442e42bb784a16e22d1aac48b2057f2\t\tmm_32360911_3244267_11031966\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282163096439\t1")
                ),
                Lists.<Pair<NullWritable, Text>>newArrayList(
                    new Pair<>(NullWritable.get(), new Text("a15cd8d54858393cd0df1bd43071b7a4\t20130606000104300\tVhkrPimvL6LftCL\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022),gzip(gfe),gzip(gfe)\t120.86.57.*\t216\t227\t2\teS8-QxdaMNf7gspy\t7feb5935d5a649fe6891181dce21a524\t\t3146654513\t728\t90\t2\t0\t5\t48f2e9ba15708c0146bda5e1dd653caa\t300\t1458\t282162993770\t0")),
                    new Pair<>(NullWritable.get(), new Text("586f49e7e088a720449301741e03217e\t20130606000104300\tVhkrPimvL6LftCL\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727),gzip(gfe),gzip(gfe)\t60.222.234.*\t15\t23\t2\tDF9blS9YBquIgqK4wJB\t6120749d193c13bbca36b57540241b31\t\t3261131688\t300\t250\t2\t0\t5\td881a6c788e76c2c27ed1ef04f119544\t238\t3358\t282825712806\t1")),
                    new Pair<>(NullWritable.get(), new Text("f2e3a74bc9c17e750ca47a6722c11a95\t20130606000104300\tVhkrPimvL6LftCL\tmozilla/5.0 (ipod; cpu iphone os 6_0 like mac os x) applewebkit/536.26 (khtml, like gecko) mobile/10a406\t123.149.217.*\t164\t165\t1\ttrqRTuu9BqshwMB4JKTI\t442e42bb784a16e22d1aac48b2057f2\t\tmm_32360911_3244267_11031966\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282163096439\t1"))

                )
            }
        };
    }

}
