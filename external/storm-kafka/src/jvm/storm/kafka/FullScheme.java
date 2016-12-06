package storm.kafka;

import backtype.storm.spout.Scheme;

import java.nio.ByteBuffer;
import java.util.List;


public interface FullScheme extends Scheme {
    List<Object> deserialize(ByteBuffer key, ByteBuffer value, Partition partition, long offset);
}
