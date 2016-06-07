package org.apache.storm.kafka;

import org.apache.storm.spout.Scheme;

import java.nio.ByteBuffer;
import java.util.List;


public interface FullScheme extends Scheme {
    List<Object> deserialize(ByteBuffer key, ByteBuffer value, Partition partition, long offset);
}
