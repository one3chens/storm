package org.apache.storm.kafka;


import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class FullSchemeAsMultiScheme extends SchemeAsMultiScheme {
    public FullSchemeAsMultiScheme(Scheme scheme) {
        super(scheme);
    }

    public Iterable<List<Object>> deserialize(ByteBuffer key, ByteBuffer value, Partition partition, long offset) {
        List<Object> o = ((FullScheme) scheme).deserialize(key, value, partition, offset);
        if (o == null) {
            return null;
        } else {
            return Arrays.asList(o);
        }
    }
}
