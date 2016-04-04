package com.epam.bdc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author zenind
 */
public class IPinYouWritable implements WritableComparable<IPinYouWritable> {

    private Text id;

    private Text timestamp;

    public IPinYouWritable() {
        this.id = new Text();
        this.timestamp = new Text();
    }

    public IPinYouWritable(Text id, Text timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(IPinYouWritable o) {
        int cmp = id.compareTo(o.getId());
        return cmp != 0 ? cmp : timestamp.compareTo(o.getTimestamp());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IPinYouWritable that = (IPinYouWritable) o;
        return Objects.equals(id, that.id) &&
            Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp);
    }

    @Override
    public String toString() {
        return "IPinYouWritable{" + "id=" + id + ", timestamp=" + timestamp + '}';
    }
}
