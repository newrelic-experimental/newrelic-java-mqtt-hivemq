package com.hivemq.client.internal.mqtt.handler.publish.incoming;

import java.util.HashMap;

import org.reactivestreams.Subscriber;

import com.hivemq.client.internal.mqtt.MqttClientConfig;
import com.hivemq.client.internal.mqtt.handler.util.FlowWithEventLoop;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageConsumeParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.NewField;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.InboundWrapper;
import com.nr.instrumentation.hivemq.client.Utils;

@Weave(type=MatchType.BaseClass)
public abstract class MqttIncomingPublishFlow extends FlowWithEventLoop {
	
	@NewField
	private Token token = null;

	MqttIncomingPublishFlow(final Subscriber<? super Mqtt5Publish> subscriber, final MqttClientConfig clientConfig,final MqttIncomingQosHandler incomingQosHandler) { 
		super(clientConfig);
	}

	@Trace(async=true)
	public void onNext(Mqtt5Publish result) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addMQTTPublish5(attributes, result);
		NewRelic.getAgent().getTracedMethod().addCustomAttributes(attributes);
		if(token !=null) {
			token.link();
		}
		MqttTopic topic = result.getTopic();
		String topicName = topic.toString().replace('/', '_');
		Mqtt5UserProperties userProperties = result.getUserProperties();
		MessageConsumeParameters params = MessageConsumeParameters.library("HiveMQ").destinationType(DestinationType.NAMED_TOPIC).destinationName(topicName).inboundHeaders(new InboundWrapper(userProperties)).build();
		
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		Weaver.callOriginal();
	}
	
	public void onComplete() {
		if(token != null) {
			token.expire();
			token = null;
		}
		Weaver.callOriginal();
	}
	
	public void onError(Throwable t) {
		NewRelic.noticeError(t);
		if(token != null) {
			token.expire();
			token = null;
		}
		Weaver.callOriginal();
	}
	
	@Trace
	public void request(long n) {
		if(token == null) {
			Token t = NewRelic.getAgent().getTransaction().getToken();
			if(t != null && t.isActive()) {
				token = t;
			} else if(t != null) {
				t.expire();
				t = null;
			}
		}
		Weaver.callOriginal();
	}
	
	protected void onCancel() {
		if(token != null) {
			token.expire();
			token = null;
		}
		Weaver.callOriginal();
	}
	
	void runCancel() {
		if(token != null) {
			token.expire();
			token = null;
		}
		Weaver.callOriginal();
	}
}
