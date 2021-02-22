package com.hivemq.client.internal.mqtt.handler.publish.outgoing;

import org.reactivestreams.Subscriber;

import com.hivemq.client.internal.mqtt.MqttClientConfig;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.SubscriberWrapper;

import io.reactivex.Flowable;

@Weave
public class MqttIncomingAckFlowable {
	
	
	public MqttIncomingAckFlowable(Flowable<MqttPublish> publishFlowable, MqttClientConfig clientConfig) {
		
	}

	@Trace(async=true)
	protected void subscribeActual(Subscriber<? super Mqtt5PublishResult> subscriber) {
		Token t = NewRelic.getAgent().getTransaction().getToken();
		Token token = null;
		if(t != null && t.isActive()) {
			token = t;
		} else if(t != null) {
			t.expire();
			t = null;
		}
		SubscriberWrapper wrapper = new SubscriberWrapper(subscriber, token);
		subscriber = wrapper;
		Weaver.callOriginal();

	}
}
