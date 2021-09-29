package com.hivemq.client.internal.mqtt.handler.publish.incoming;

import java.util.logging.Level;

import org.reactivestreams.Subscriber;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.SubscriberWrapper;

import io.reactivex.Flowable;

@Weave
public abstract class MqttGlobalIncomingPublishFlowable extends Flowable<Mqtt5Publish> {
	
	protected void subscribeActual(Subscriber<? super Mqtt5Publish> subscriber) {
		NewRelic.getAgent().getLogger().log(Level.FINE, "call to MqttGlobalIncomingPublishFlowable.subscribeActual({0})", subscriber.getClass().getName());
		if(!(subscriber instanceof SubscriberWrapper)) {
			SubscriberWrapper<Mqtt5Publish> wrapper = new SubscriberWrapper<Mqtt5Publish>(subscriber);
			subscriber = wrapper;
		}
		
		Weaver.callOriginal();
	}

}
