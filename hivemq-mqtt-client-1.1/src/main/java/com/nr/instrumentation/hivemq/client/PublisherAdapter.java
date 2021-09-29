package com.nr.instrumentation.hivemq.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.hivemq.client.internal.mqtt.datatypes.MqttTopicImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertyImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUtf8StringImpl;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;

import io.reactivex.functions.Function;

public class PublisherAdapter implements Function<Mqtt5Publish, Mqtt5Publish> {
	

	private static boolean isTransformed = false;
	
	private Token token = null;
	
	public PublisherAdapter() {
		token = NewRelic.getAgent().getTransaction().getToken();
		if(!isTransformed) {
			isTransformed = true;
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
		}
	}

	@Override
	@Trace(async=true)
	public  Mqtt5Publish  apply(Mqtt5Publish source) throws Exception {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		Mqtt5UserProperties userProperties = source.getUserProperties();
		
		MqttUserPropertiesImpl userPropertiesNew = convertProperties(userProperties);
		
		OutboundWrapper wrapper = new OutboundWrapper(userPropertiesNew);
		NewRelic.getAgent().getTracedMethod().addOutboundRequestHeaders(wrapper);
		userPropertiesNew = wrapper.getCurrent();
		
		MqttTopic topic = source.getTopic();
		
		MqttTopicImpl topicImpl = (MqttTopicImpl)topic;
		ByteBuffer payload = source.getPayload().isPresent() ? source.getPayload().get() : null;
		
		long expiry = source.getMessageExpiryInterval().isPresent() ? source.getMessageExpiryInterval().getAsLong() : MqttPublish.NO_MESSAGE_EXPIRY;
		
		Mqtt5PayloadFormatIndicator format =  source.getPayloadFormatIndicator().isPresent() ? source.getPayloadFormatIndicator().get() : null;
		
		MqttUtf8String ct = source.getContentType().isPresent() ? source.getContentType().get() : null;
		
		MqttTopicImpl response = source.getResponseTopic().isPresent() ? (MqttTopicImpl)source.getResponseTopic().get() : null;
				
		ByteBuffer correlationData = source.getCorrelationData().isPresent() ? source.getCorrelationData().get() : null;
		MqttPublish pub = new MqttPublish(topicImpl, payload, source.getQos(), source.isRetain(), expiry, format, (MqttUtf8StringImpl) ct, response, correlationData, userPropertiesNew);
		return pub;
	}

	private MqttUserPropertiesImpl convertProperties(Mqtt5UserProperties props) {
		MqttUserPropertiesImpl userProps = null;
		
		
		
		if(props instanceof MqttUserPropertiesImpl) {
			userProps = (MqttUserPropertiesImpl)props;
		} else {
			List<? extends Mqtt5UserProperty> properties = props.asList();
			List<MqttUserPropertyImpl> list2 = new ArrayList<MqttUserPropertyImpl>();
			for(Mqtt5UserProperty prop : properties) {
				
				MqttUserPropertyImpl uProp = MqttUserPropertyImpl.of(prop.getName().toString(), prop.getValue().toString());
				list2.add(uProp);
			}
			ImmutableList<MqttUserPropertyImpl> list = ImmutableList.copyOf(list2);
			userProps = MqttUserPropertiesImpl.of(list);
		}
		
		return userProps;
	}
}
