package com.nr.instrumentation.hivemq.client;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageConsumeParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TracedMethod;
import com.newrelic.api.agent.TransactionNamePriority;

public class SubscriberWrapper<T> implements Subscriber<T> {
	
	private Subscriber<? super T> delegate = null;
	private static boolean isTransformed = false;
	
	public SubscriberWrapper(Subscriber<? super T> d) {
		delegate = d;
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
	@Trace(dispatcher=true)
	public void onNext(T t) {
		TracedMethod traced = NewRelic.getAgent().getTracedMethod();
		
		if (t instanceof Mqtt5Publish) {
			Mqtt5Publish result = (Mqtt5Publish)t;
			String topicName = result.getTopic().toString();
			MessageConsumeParameters params = MessageConsumeParameters.library("HiveMQ")
					.destinationType(DestinationType.NAMED_TOPIC).destinationName(topicName).inboundHeaders(new InboundWrapper(result.getUserProperties()))
					.build();
			traced.reportAsExternal(params);
			NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.FRAMEWORK_HIGH, false, "HiveMQ", new String[] {"HiveMQ","Take",topicName});
		}
		delegate.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		delegate.onError(t);
	}

	@Override
	public void onComplete() {
		delegate.onComplete();
	}

}
