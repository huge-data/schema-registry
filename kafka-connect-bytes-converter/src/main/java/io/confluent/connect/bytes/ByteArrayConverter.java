/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.bytes;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;


/**
 * {@link Converter} implementation that only supports serializing to strings. When converting Kafka Connect data to bytes,
 * the schema will be ignored and {@link Object#toString()} will always be invoked to convert the data to a String.
 * When converting from bytes to Kafka Connect format, the converter will only ever return an optional string schema and
 * a string or null.
 *
 * Encoding configuration is identical to {@link StringSerializer} and {@link StringDeserializer}, but for convenience
 * this class can also be configured to use the same encoding for both encoding and decoding with the converter.encoding
 * setting.
 */
public class ByteArrayConverter implements Converter {
	private final ByteArraySerializer serializer = new ByteArraySerializer();
	private final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

	public ByteArrayConverter() {

    }

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Map<String, Object> serializerConfigs = new HashMap<>();
		serializerConfigs.putAll(configs);
		Map<String, Object> deserializerConfigs = new HashMap<>();
		deserializerConfigs.putAll(configs);

		serializer.configure(serializerConfigs, isKey);
		deserializer.configure(deserializerConfigs, isKey);
    }

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		try {
			return serializer.serialize(topic, (byte[]) value);
		} catch (SerializationException e) {
			throw new DataException("Failed to serialize to a string: ", e);
		}
    }

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		try {
			return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(deserializer.deserialize(topic,
					value)));
		} catch (SerializationException e) {
			throw new DataException("Failed to deserialize string: ", e);
		}
    }
}
