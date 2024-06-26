package com.nr.instrumentation.hivemq.client;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

public class Utils {
	
	public static void addMQTTPublish5(Map<String, Object> attributes, Mqtt5Publish publish) {
		Optional<MqttUtf8String> cType = publish.getContentType();
		if(cType.isPresent()) {
			addAttribute(attributes, "ContentType", cType.get());
		}
		Optional<ByteBuffer> correlation = publish.getCorrelationData();
		if(correlation.isPresent()) {
			addAttribute(attributes, "CorrelationData", correlation.get());
		}
		MqttTopic topic = publish.getTopic();
		addAttribute(attributes, "Topic", topic);
		Optional<MqttTopic> responseTopic = publish.getResponseTopic();
		if(responseTopic.isPresent()) {
			addAttribute(attributes, "ResponseTopic", responseTopic.get());
		}
		addAttribute(attributes, "QOS", publish.getQos());
		addAttribute(attributes, "MessageType", publish.getType());
		
	}
	
	public static void addAttribute(Map<String,Object> attributes, String key, Object value) {
		if(key != null && !key.isEmpty() && value != null) {
			attributes.put(key, value);
		}
	}

}
