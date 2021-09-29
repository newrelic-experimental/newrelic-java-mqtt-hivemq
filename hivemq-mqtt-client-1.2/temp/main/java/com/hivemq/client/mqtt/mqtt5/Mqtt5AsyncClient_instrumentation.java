package com.hivemq.client.mqtt.mqtt5;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.PublisherConsumerWrapper;

@Weave(type=MatchType.Interface,originalName="com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient")
public abstract class Mqtt5AsyncClient_instrumentation {

	@Trace
	public void publishes(MqttGlobalPublishFilter filter, Consumer<Mqtt5Publish> callback, Executor executor) {
		if(!(callback instanceof PublisherConsumerWrapper)) {
			callback = new PublisherConsumerWrapper(callback);
		}
		Weaver.callOriginal();
	}
	
	@Trace
	public void publishes(MqttGlobalPublishFilter filter, Consumer<Mqtt5Publish> callback) {
		if(!(callback instanceof PublisherConsumerWrapper)) {
			callback = new PublisherConsumerWrapper(callback);
		}
		Weaver.callOriginal();
	}
	
}
