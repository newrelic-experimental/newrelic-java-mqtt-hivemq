package com.nr.instrumentation.hivemq.client;

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
		return new Mqtt5PublisherWrapper(source);
	}

}
