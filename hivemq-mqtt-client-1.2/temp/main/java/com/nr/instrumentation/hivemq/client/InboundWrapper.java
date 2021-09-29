package com.nr.instrumentation.hivemq.client;

import java.util.List;

import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.newrelic.api.agent.HeaderType;
import com.newrelic.api.agent.InboundHeaders;

public class InboundWrapper implements InboundHeaders {
	
	Mqtt5UserProperties userProperties = null;

	public InboundWrapper(Mqtt5UserProperties userProperties) {
		this.userProperties = userProperties;
	}

	@Override
	public String getHeader(String name) {
		List<? extends Mqtt5UserProperty> list = userProperties.asList();
		MqttUtf8String searchFor = MqttUtf8String.of(name);
		for(Mqtt5UserProperty property : list) {
			MqttUtf8String propertyName = property.getName();
			if(propertyName.equals(searchFor)) {
				MqttUtf8String value = property.getValue();
				if(value != null) {
					return value.toString();
				}
			}
		}
		return null;
	}

	@Override
	public HeaderType getHeaderType() {
		return HeaderType.MESSAGE;
	}

}
