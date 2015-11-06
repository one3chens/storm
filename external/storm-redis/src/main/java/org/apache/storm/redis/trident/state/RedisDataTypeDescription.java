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

import java.io.Serializable;

/**
 * RedisDataTypeDescription defines the Redis data type and an additional key factory for the key name with hash and sorted set types
 */
public class RedisDataTypeDescription implements Serializable {
    public enum RedisDataType { STRING, HASH, LIST, SET, SORTED_SET, HYPER_LOG_LOG }

    private RedisDataType dataType;
    private KeyFactory fieldNameFactory;

    /**
     * Constructor
     * @param dataType data type
     */
    public RedisDataTypeDescription(RedisDataType dataType) {
        this(dataType, null);
    }

    /**
     * Constructor
     * @param dataType data type
     * @param fieldNameFactory key factory for the key name in hash and sorted set types
     */
    public RedisDataTypeDescription(RedisDataType dataType, KeyFactory fieldNameFactory) {
        this.dataType = dataType;
        this.fieldNameFactory = fieldNameFactory;

        if (dataType == RedisDataType.HASH || dataType == RedisDataType.SORTED_SET) {
            if (fieldNameFactory == null) {
                throw new IllegalArgumentException("Hash and Sorted Set data types should have a key factory for the field name");
            }
        }
    }

    /**
     * Returns defined data type.
     * @return data type
     */
    public RedisDataType getDataType() {
        return dataType;
    }

    /**
     * Returns defined additional key.
     * @return additional key
     */
    public KeyFactory getFieldNameFactory() {
        return fieldNameFactory;
    }
}