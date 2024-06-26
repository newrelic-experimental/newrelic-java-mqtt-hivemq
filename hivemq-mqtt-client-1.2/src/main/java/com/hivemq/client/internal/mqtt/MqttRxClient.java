package com.hivemq.client.internal.mqtt;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.PublisherAdapter;

import io.reactivex.Flowable;

@Weave
public abstract class MqttRxClient {

	@Trace
	public  Flowable<Mqtt5PublishResult> publish(Flowable<Mqtt5Publish> publishFlowable) {
		publishFlowable = publishFlowable.map(new PublisherAdapter());
		return Weaver.callOriginal();
	}
}
