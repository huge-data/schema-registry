
package io.confluent.connect.string;

import static org.junit.Assert.assertEquals;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

// AvroConverter is a trivial combination of the serializers and the AvroData conversions, so
// most testing is performed on AvroData since it is much easier to compare the results in Avro
// runtime format than in serialized form. This just adds a few sanity checks to make sure things
// work end-to-end.
public class StringConverterTest {
	private static final String TOPIC = "topic";

	private final SchemaRegistryClient schemaRegistry;
	private final StringConverter converter;

	public StringConverterTest() {
		schemaRegistry = new MockSchemaRegistryClient();
		converter = new StringConverter();
	}

	@Before
	public void setUp() {
		converter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
	}

	@Test
	public void test() {
		SchemaAndValue original = new SchemaAndValue(Schema.STRING_SCHEMA, "123123123");
		byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
		SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
		// Because of registration in schema registry and lookup, we'll have added a version number
		SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.string().version(null).build(), "123123123");
		assertEquals(expected, schemaAndValue);
	}


}
