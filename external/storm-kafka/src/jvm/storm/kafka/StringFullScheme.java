package storm.kafka;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

public class StringFullScheme extends StringScheme implements FullScheme {
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String TOPIC_FIELD = "topic";
    public static final String PARTITION_FIELD = "partition";
    public static final String OFFSET_FIELD = "offset";

    @Override
    public List<Object> deserialize(ByteBuffer key, ByteBuffer value, Partition partition, long offset) {
        String stringKey = StringScheme.deserializeString(key.array());
        String stringValue = StringScheme.deserializeString(value.array());
        return new Values(stringKey, stringValue, partition.topic, partition.partition, offset);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(KEY_FIELD, VALUE_FIELD, TOPIC_FIELD, PARTITION_FIELD, OFFSET_FIELD);
    }
}
