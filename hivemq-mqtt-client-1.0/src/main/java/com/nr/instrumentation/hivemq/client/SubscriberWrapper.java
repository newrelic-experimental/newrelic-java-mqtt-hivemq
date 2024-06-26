package com.nr.instrumentation.hivemq.client;

import java.util.HashMap;
import java.util.Optional;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageProduceParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Segment;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;

public class SubscriberWrapper implements Subscriber<Mqtt5PublishResult> {
	
	private Subscriber<? super Mqtt5PublishResult> delegate = null;
	private Token token = null;
	private static boolean isTransformed = false;
	private Segment segment = null;
	
	public SubscriberWrapper(Subscriber<? super Mqtt5PublishResult> d, Token t) {
		delegate = d;
		token = t;
		segment = NewRelic.getAgent().getTransaction().startSegment("HiveMQ-Publish");
		if(!isTransformed) {
			isTransformed = true;
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		delegate.onSubscribe(s);
	}

	@Override
	@Trace(async=true)
	public void onNext(Mqtt5PublishResult t) {
		HashMap<String, Object>  attributes = new HashMap<String, Object>();
		if(token != null) {
			token.link();
		}
		Optional<Throwable> error = t.getError();
		if(error.isPresent()) {
			NewRelic.noticeError(error.get());
		}
		Mqtt5Publish result = t.getPublish();
		Utils.addMQTTPublish5(attributes, result);
		NewRelic.getAgent().getTracedMethod().addCustomAttributes(attributes);
		String topicName = result.getTopic().toString();
		MessageProduceParameters params = MessageProduceParameters.library("HiveMQ").destinationType(DestinationType.NAMED_TOPIC).destinationName(topicName).outboundHeaders(null).build();
		if(segment != null) {
			segment.reportAsExternal(params);
			segment.end();
			segment = null;
		} else {
			NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		}
		delegate.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		if(token != null) {
			token.expire();
			token = null;
		}
		delegate.onError(t);
	}

	@Override
	public void onComplete() {
		if(token != null) {
			token.expire();
			token = null;
		}
		delegate.onComplete();
	}

}
