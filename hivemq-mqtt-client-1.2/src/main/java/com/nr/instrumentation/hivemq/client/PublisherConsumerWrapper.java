package com.nr.instrumentation.hivemq.client;

import java.util.function.Consumer;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageConsumeParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

public class PublisherConsumerWrapper implements Consumer<Mqtt5Publish>  {
	
	private Consumer<Mqtt5Publish> delegate = null;
	private static boolean isTransformed = false;
	
	public PublisherConsumerWrapper(Consumer<Mqtt5Publish> d) {
		delegate = d;
		if(!isTransformed) {
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
			isTransformed = true;
		}
	}

	@Override
	@Trace(dispatcher=true)
	public void accept(Mqtt5Publish t) {
		NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.FRAMEWORK_HIGH, false, "HiveMQ", "HiveMQ","Topic","Receive",t.getTopic().toString());
		MessageConsumeParameters params = MessageConsumeParameters.library("HiveMQ").destinationType(DestinationType.NAMED_TOPIC).destinationName(t.getTopic().toString()).inboundHeaders(new InboundWrapper(t.getUserProperties())).build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		delegate.accept(t);
	}

}
