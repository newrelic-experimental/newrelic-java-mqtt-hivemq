package com.nr.instrumentation.hivemq.client;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder.Complete;
import com.newrelic.api.agent.NewRelic;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5WillPublish;

public class Mqtt5PublisherWrapper implements Mqtt5Publish {
	
	private Mqtt5Publish delegate = null;
	
	public Mqtt5PublisherWrapper(Mqtt5Publish p) {
		delegate = p;
	}

	@Override
	public MqttTopic getTopic() {
		return delegate.getTopic();
	}

	@Override
	public Optional<ByteBuffer> getPayload() {
		return delegate.getPayload();
	}

	@Override
	public byte[] getPayloadAsBytes() {
		return delegate.getPayloadAsBytes();
	}

	@Override
	public MqttQos getQos() {
		return delegate.getQos();
	}

	@Override
	public boolean isRetain() {
		return delegate.isRetain();
	}

	@Override
	public OptionalLong getMessageExpiryInterval() {
		return delegate.getMessageExpiryInterval();
	}

	@Override
	public Optional<Mqtt5PayloadFormatIndicator> getPayloadFormatIndicator() {
		return delegate.getPayloadFormatIndicator();
	}

	@Override
	public Optional<MqttUtf8String> getContentType() {
		return delegate.getContentType();
	}

	@Override
	public Optional<MqttTopic> getResponseTopic() {
		return delegate.getResponseTopic();
	}

	@Override
	public Optional<ByteBuffer> getCorrelationData() {
		return delegate.getCorrelationData();
	}

	@Override
	public Mqtt5UserProperties getUserProperties() {
		Mqtt5UserProperties props = delegate.getUserProperties();
		if(props instanceof MqttUserPropertiesImpl) {
			MqttUserPropertiesImpl propsImpl = (MqttUserPropertiesImpl)props;
			OutboundWrapper wrapper = new OutboundWrapper(propsImpl);
			NewRelic.getAgent().getTracedMethod().addOutboundRequestHeaders(wrapper);
			return wrapper.getCurrent();
		}
		return props;
	}

	@Override
	public Mqtt5WillPublish asWill() {
		return delegate.asWill();
	}

	@Override
	public Complete extend() {
		return delegate.extend();
	}

}
