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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateType;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * AbstractRedisMapState is base class of any RedisMapState, which implements IBackingMap.
 * <p></p>
 * Derived classes should provide
 * <ul>
 * <li>which Serializer it uses</li>
 * <li>which KeyFactory it uses</li>
 * <li>how to retrieve values from Redis</li>
 * <li>how to store values to Redis</li>
 * </ul>
 * and AbstractRedisMapState takes care of rest things.
 *
 * @param <T> value's type class
 */
public abstract class AbstractRedisMapState<T> implements IBackingMap<T> {
	public static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
			StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
			StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
			StateType.OPAQUE, new JSONOpaqueSerializer()
	));

	protected List<String> buildKeys(KeyFactory keyFactory, List<List<Object>> keys) {
		List<String> stringKeys = new ArrayList<String>();

		for (List<Object> key : keys) {
			stringKeys.add(keyFactory.build(key));
		}

		return stringKeys;
	}

	protected List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
		List<T> result = new ArrayList<T>(keys.size());
		for (String value : values) {
			if (value != null) {
				result.add((T) getSerializer().deserialize(value.getBytes()));
			} else {
				result.add(null);
			}
		}
		return result;
	}

	/**
	 * Returns Serializer which is used for serializing tuple value and deserializing Redis value.
	 *
	 * @return serializer
	 */
	protected abstract Serializer getSerializer();
}
