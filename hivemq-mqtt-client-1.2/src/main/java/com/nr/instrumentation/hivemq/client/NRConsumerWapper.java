package com.nr.instrumentation.hivemq.client;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageConsumeParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

import io.reactivex.functions.Consumer;

public class NRConsumerWapper<F> implements Consumer<F> {
	
	private Consumer<? super F> delegate = null;
	private static boolean isTransformed = false;
	
	
	public NRConsumerWapper(Consumer<? super F> d) {
		delegate = d;
		if(!isTransformed) {
			isTransformed = true;
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
		}
	}

	@Override
	@Trace(dispatcher=true)
	public void accept(F t) throws Exception {
		if(t instanceof Mqtt5Publish) {
			Mqtt5Publish publish = (Mqtt5Publish)t;
			NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.FRAMEWORK_HIGH, false, "HiveMQ", "HiveMQ","Topic","Receive",publish.getTopic().toString());
			MessageConsumeParameters params = MessageConsumeParameters.library("HiveMQ").destinationType(DestinationType.NAMED_TOPIC).destinationName(publish.getTopic().toString()).inboundHeaders(new InboundWrapper(publish.getUserProperties())).build();
			NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		}
		if(delegate != null) {
			delegate.accept(t);
		}
	}

}
