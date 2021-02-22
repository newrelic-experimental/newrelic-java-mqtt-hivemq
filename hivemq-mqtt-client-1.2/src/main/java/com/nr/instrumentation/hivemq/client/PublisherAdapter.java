package com.nr.instrumentation.hivemq.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertyImpl;
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
		
		Optional<MqttUtf8String> contentOption = source.getContentType();
		MqttUtf8String ct = contentOption.isPresent() ? contentOption.get() : null;
		Optional<ByteBuffer> corrOption = source.getCorrelationData();
		ByteBuffer correlationData = corrOption.isPresent() ? corrOption.get() : null;
		OptionalLong expiryOption = source.getMessageExpiryInterval(); 
		Long expiry = expiryOption.isPresent() ? expiryOption.getAsLong() : null;
		Optional<ByteBuffer> payloadOption = source.getPayload();
		ByteBuffer payload = payloadOption.isPresent() ? payloadOption.get() : null;

		Optional<Mqtt5PayloadFormatIndicator> formatOption = source.getPayloadFormatIndicator();
		Mqtt5PayloadFormatIndicator payloadFormatIndicator = formatOption.isPresent() ? formatOption.get() : null;
		
		Optional<MqttTopic> responseOption = source.getResponseTopic();
		MqttTopic response = responseOption.isPresent() ? responseOption.get() : null;
		
		if(expiry != null) {
			return Mqtt5Publish.builder().topic(source.getTopic()).contentType(ct).correlationData(correlationData).messageExpiryInterval(expiry)
				.payload(payload).payloadFormatIndicator(payloadFormatIndicator).qos(source.getQos()).responseTopic(response).retain(source.isRetain())
				.userProperties(getUserProperties(source)).build();
		} else {
			return Mqtt5Publish.builder().topic(source.getTopic()).contentType(ct).correlationData(correlationData).noMessageExpiry()
					.payload(payload).payloadFormatIndicator(payloadFormatIndicator).qos(source.getQos()).responseTopic(response).retain(source.isRetain())
					.userProperties(getUserProperties(source)).build();
		}
	}

	private Mqtt5UserProperties getUserProperties(Mqtt5Publish delegate) {
		Mqtt5UserProperties props = delegate.getUserProperties();
		if(props == null) {
			MqttUserPropertiesImpl propsImpl = MqttUserPropertiesImpl.NO_USER_PROPERTIES;
			OutboundWrapper wrapper = new OutboundWrapper(propsImpl);
			NewRelic.getAgent().getTracedMethod().addOutboundRequestHeaders(wrapper);
			return wrapper.getCurrent();
			
		}
		if(props instanceof MqttUserPropertiesImpl) {
			MqttUserPropertiesImpl propsImpl = (MqttUserPropertiesImpl)props;
			OutboundWrapper wrapper = new OutboundWrapper(propsImpl);
			NewRelic.getAgent().getTracedMethod().addOutboundRequestHeaders(wrapper);
			return wrapper.getCurrent();
		} else {
			List<? extends Mqtt5UserProperty> current = props.asList();
			List<MqttUserPropertyImpl> list = new ArrayList<MqttUserPropertyImpl>();
			for(Mqtt5UserProperty prop : current) {
				MqttUserPropertyImpl newProp = MqttUserPropertyImpl.of(prop.getName().toString(), prop.getValue().toString());
				list.add(newProp);
			}
			ImmutableList<MqttUserPropertyImpl> iList = ImmutableList.copyOf(list);
			MqttUserPropertiesImpl propsImpl = MqttUserPropertiesImpl.of(iList);
			OutboundWrapper wrapper = new OutboundWrapper(propsImpl);
			NewRelic.getAgent().getTracedMethod().addOutboundRequestHeaders(wrapper);
			return wrapper.getCurrent();
		}
	}

}
