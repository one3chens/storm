/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.redis.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;

import java.util.*;

/**
 * IBackingMap implementation for single Redis environment.
 *
 * @param <T> value's type class
 * @see AbstractRedisMapState
 */
public class RedisMapState<T> extends AbstractRedisMapState<T> {
    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig) {
        return opaque(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return opaque(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return opaque(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, Options<OpaqueValue> opts) {
        return new Factory(jedisPoolConfig, StateType.OPAQUE, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig) {
        return transactional(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return transactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return transactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, Options<TransactionalValue> opts) {
        return new Factory(jedisPoolConfig, StateType.TRANSACTIONAL, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig) {
        return nonTransactional(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return nonTransactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return nonTransactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, Options<Object> opts) {
        return new Factory(jedisPoolConfig, StateType.NON_TRANSACTIONAL, opts);
    }

    /**
     * RedisMapState.Factory provides single Redis environment version of StateFactory.
     */
    protected static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        JedisPoolConfig jedisPoolConfig;

        StateType type;
        Options options;

        /**
         * Constructor
         *
         * @param jedisPoolConfig configuration for JedisPool
         * @param type StateType
         * @param options options of State
         */
        public Factory(JedisPoolConfig jedisPoolConfig, StateType type, Options options) {
            this.jedisPoolConfig = jedisPoolConfig;
            this.type = type;
            this.options = options;

            if (options.keyFactory == null) {
                options.keyFactory = new KeyFactory.DefaultKeyFactory();
            }
            if (options.serializer == null) {
                options.serializer = DEFAULT_SERIALIZERS.get(type);
                if (options.serializer == null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,
                                                    jedisPoolConfig.getHost(),
                                                    jedisPoolConfig.getPort(),
                                                    jedisPoolConfig.getTimeout(),
                                                    jedisPoolConfig.getPassword(),
                                                    jedisPoolConfig.getDatabase());
            RedisMapState state = new RedisMapState(jedisPool, options);
            CachedMap c = new CachedMap(state, options.localCacheSize);

            MapState ms;
            if (type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);

            } else if (type == StateType.OPAQUE) {
                ms = OpaqueMap.build(c);

            } else if (type == StateType.TRANSACTIONAL) {
                ms = TransactionalMap.build(c);

            } else {
                throw new RuntimeException("Unknown state type: " + type);
            }

            return new SnapshottableMap(ms, new Values(options.globalKey));
        }
    }

    protected class FieldPair {
        public int index;
        public String fieldName;

        public FieldPair(int index, String fieldName) {
            this.index = index;
            this.fieldName = fieldName;
        }
    }

    private JedisPool jedisPool;
    private Options options;

    /**
     * Constructor
     *
     * @param jedisPool JedisPool
     * @param options options of State
     */
    public RedisMapState(JedisPool jedisPool, Options options) {
        this.jedisPool = jedisPool;
        this.options = options;
    }

    private String[] buildKeyValuesList(Map<String, String> keyValues) {
        String[] keyValueLists = new String[keyValues.size() * 2];

        int idx = 0;
        for (Map.Entry<String, String> kvEntry : keyValues.entrySet()) {
            keyValueLists[idx++] = kvEntry.getKey();
            keyValueLists[idx++] = kvEntry.getValue();
        }

        return keyValueLists;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializer getSerializer() {
        return this.options.serializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size() == 0) {
            return Collections.emptyList();
        }

        RedisDataTypeDescription description = this.options.dataTypeDescription;
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            switch (description.getDataType()) {
                case STRING:
                    List<String> stringKeys = buildKeys(this.options.keyFactory, keys);
                    String[] keysArray = stringKeys.toArray(new String[stringKeys.size()]);

                    return deserializeValues(keys, jedis.mget(keysArray));
                case HASH:
                    // This assumes that more hgets are more expensive than fewer hmgets,
                    // but it's more complex and requires more processing here
                    Map<String, List<FieldPair>> hashMap = new HashMap<>();
                    List<String> values = new ArrayList<>(keys.size());

                    for(int i = 0; i < keys.size(); i++) {
                        values.add(null);
                        String keyName = this.options.keyFactory.build(keys.get(i));
                        String fieldName = description.getFieldNameFactory().build(keys.get(i));

                        FieldPair field = new FieldPair(i, fieldName);

                        if(hashMap.containsKey(keyName))
                            hashMap.get(keyName).add(field);
                        else {
                            List<FieldPair> list = new ArrayList<>();
                            list.add(field);
                            hashMap.put(keyName, list);
                        }
                    }

                    for (Map.Entry<String, List<FieldPair>> entry: hashMap.entrySet()) {
                        String[] fieldsArray = new String[entry.getValue().size()];
                        for(int i = 0; i < entry.getValue().size(); i++)
                            fieldsArray[i] = entry.getValue().get(i).fieldName;
                        List<String> tempValues = jedis.hmget(entry.getKey(), fieldsArray);
                        for(int i = 0; i < tempValues.size(); i++)
                            values.set(entry.getValue().get(i).index, tempValues.get(i));
                    }

                    return deserializeValues(keys, values);
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + description.getDataType());
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        if (keys.size() == 0) {
            return;
        }

        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            RedisDataTypeDescription description = this.options.dataTypeDescription;

            switch (description.getDataType()) {
                case STRING:
                    Map<String, String> stringMap = new HashMap<>();

                    for (int i = 0; i < keys.size(); i++) {
                        String val = new String(this.options.serializer.serialize(vals.get(i)));
                        String redisKey = this.options.keyFactory.build(keys.get(i));
                        stringMap.put(redisKey, val);
                    }

                    String[] keyValue = buildKeyValuesList(stringMap);
                    jedis.mset(buildKeyValuesList(stringMap));

                    if(this.options.expireIntervalSec > 0){
                        Pipeline pipe = jedis.pipelined();
                        for(int i = 0; i < keyValue.length; i += 2){
                            pipe.expire(keyValue[i], this.options.expireIntervalSec);
                        }
                        pipe.sync();
                    }

                    break;
                case HASH:
                    Map<String, Map<String, String>> hashMap = new HashMap<>();

                    for (int i = 0; i < keys.size(); i++) {
                        String val = new String(this.options.serializer.serialize(vals.get(i)));
                        String keyName = this.options.keyFactory.build(keys.get(i));
                        String fieldName = description.getFieldNameFactory().build(keys.get(i));

                        if(hashMap.containsKey(keyName))
                            hashMap.get(keyName).put(fieldName, val);
                        else {
                            Map<String, String> map = new HashMap<>();
                            map.put(fieldName, val);
                            hashMap.put(keyName, map);
                        }
                    }

                    for (Map.Entry<String, Map<String, String>> entry: hashMap.entrySet()) {
                        jedis.hmset(entry.getKey(), entry.getValue());

                        if (this.options.expireIntervalSec > 0) {
                            jedis.expire(entry.getKey(), this.options.expireIntervalSec);
                        }
                    }

                    break;
                default:
                    throw new IllegalArgumentException("Cannot process data type: " + description.getDataType());
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
