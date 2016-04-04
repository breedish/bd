package com.epam.bdc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zenind
 */
public class IPinYouPartitioner extends Partitioner<IPinYouWritable, Text> {

    @Override
    public int getPartition(IPinYouWritable iPinYouWritable, Text text, int numPartitions) {
        return iPinYouWritable.getId().hashCode() % numPartitions;
    }
}
