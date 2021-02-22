package com.nr.instrumentation.hivemq.client;

import java.util.logging.Level;

import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertyImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUtf8StringImpl;
import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.internal.util.collections.ImmutableList.Builder;
import com.newrelic.api.agent.HeaderType;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.OutboundHeaders;

public class OutboundWrapper implements OutboundHeaders {
	
	MqttUserPropertiesImpl userProperties = null;
	
	public OutboundWrapper(MqttUserPropertiesImpl up) {
		userProperties = up;
	}

	@Override
	public HeaderType getHeaderType() {
		return HeaderType.MESSAGE;
	}

	@Override
	public void setHeader(String name, String value) {
		MqttUtf8StringImpl nameStr = MqttUtf8StringImpl.of(name);
		MqttUtf8StringImpl valueStr = MqttUtf8StringImpl.of(value);
		MqttUserPropertyImpl property = new MqttUserPropertyImpl(nameStr, valueStr);
		
		Builder<MqttUserPropertyImpl> builder = ImmutableList.builder();
		ImmutableList<MqttUserPropertyImpl> list = builder.addAll(userProperties.asList()).add(property).build();
		userProperties = MqttUserPropertiesImpl.of(list);
		NewRelic.getAgent().getLogger().log(Level.INFO,"Set outbound headers to: {0}",userProperties);
	}

	public MqttUserPropertiesImpl getCurrent() {
		return userProperties;
	}
}
