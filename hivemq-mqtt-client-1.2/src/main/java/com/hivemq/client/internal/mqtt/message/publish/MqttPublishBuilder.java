package com.hivemq.client.internal.mqtt.message.publish;

import java.util.function.Function;

import com.hivemq.client.internal.mqtt.datatypes.MqttTopicImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.newrelic.api.agent.DestinationType;
import com.newrelic.api.agent.MessageProduceParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.OutboundWrapper;

@Weave
public class MqttPublishBuilder<B extends MqttPublishBuilder<B>> {
	
	MqttTopicImpl topic = Weaver.callOriginal();
	
	MqttUserPropertiesImpl userProperties = Weaver.callOriginal();
	
	MqttPublishBuilder() {}
	
	MqttPublishBuilder(MqttPublish publish) {
		
	}
	
	MqttPublishBuilder(MqttPublishBuilder<?> publishBuilder) {
		
	}

	@Weave
	public static class Send<P> extends Base<Send<P>> {
		
		public Send(Function<? super MqttPublish, P> parentConsumer) {
			
		}
		
		@Trace(leaf=true)
		public P send() {
			OutboundWrapper wrapper = new OutboundWrapper(userProperties);
			String topicName = topic.toString().replace('/', '_');
			MessageProduceParameters params = MessageProduceParameters.library("HiveMQ").destinationType(DestinationType.NAMED_TOPIC).destinationName(topicName).outboundHeaders(wrapper).build();
			NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
			userProperties = wrapper.getCurrent();
			return Weaver.callOriginal();
		}
	}
	
	@Weave
	private static class Base<B extends Base<B>> extends MqttPublishBuilder<B> {
		
		Base() {}

        @SuppressWarnings("unused")
		Base(MqttPublish publish) {
            super(publish);
        }
	}
	
	@Weave
	public static class Default extends Base<Default> {
		
	}
}
