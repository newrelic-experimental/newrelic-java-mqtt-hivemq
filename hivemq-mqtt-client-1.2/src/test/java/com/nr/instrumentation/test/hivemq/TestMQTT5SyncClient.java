package com.nr.instrumentation.test.hivemq;

import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertyImpl;
import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.agent.introspec.Introspector;
import com.newrelic.agent.introspec.TracedMetricData;
import com.newrelic.api.agent.Trace;

@RunWith(InstrumentationTestRunner.class)
@InstrumentationTestConfig(includePrefixes="com.hivemq.client")
public class TestMQTT5SyncClient {

	static int max = 5;
	static EmbeddedHiveMQ hiveMQ = null;
	static final String hivehost = "localhost";
	static final String syncTopic = "newrelic/test-sync";
	
	@BeforeClass
	public static void beforeClass() {
		Path path = Paths.get("./conf");
		EmbeddedHiveMQBuilder embeddedHiveMQbuilder = EmbeddedHiveMQBuilder.builder().withConfigurationFolder(path).withDataFolder(null).withExtensionsFolder(Paths.get("./extensions"));
		
		hiveMQ = embeddedHiveMQbuilder.build();
		
		hiveMQ.start().join();
		
		
	}

	@AfterClass
	public static void afterClass() {
		hiveMQ.stop().join();
	}
	
	@Test
	public void doSyncPubSubTest() {
		String txnName1 = "OtherTransaction/Custom/com.nr.instrumentation.test.hivemq.TestMQTT5SyncClient/subscribe";
		String txnName2 = "OtherTransaction/Custom/com.nr.instrumentation.test.hivemq.TestMQTT5SyncClient/publish";
		String consume = "MessageBroker/HiveMQ/Topic/Consume/Named/newrelic_test-sync";
		String publish = "MessageBroker/HiveMQ/Topic/Produce/Named/newrelic_test-sync";

		CompletableFuture<Boolean> subDone = new CompletableFuture<Boolean>();
		CompletableFuture<Boolean> pubDone = new CompletableFuture<Boolean>();
		CompletableFuture<Boolean> readyToSend = new CompletableFuture<Boolean>();
		
		SyncSubscriberThread sThread = new SyncSubscriberThread(subDone, readyToSend);
		SyncPublisherThread pThread = new SyncPublisherThread(pubDone,readyToSend);
		
		sThread.start();
		pThread.start();
		
		try {
			Boolean subFinished = subDone.get(15L, TimeUnit.SECONDS);
			System.out.println("Sub Finished: "+subFinished);
			Boolean pubFinished = pubDone.get(15L, TimeUnit.SECONDS);
			System.out.println("Pub Finished: "+pubFinished);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(15000);
		System.out.println("Finished transaction count: "+finishedTransactionCount);
		assertTrue(finishedTransactionCount == 2);
		Collection<String> txnNames = introspector.getTransactionNames();
		assertTrue(txnNames.contains(txnName1));
		assertTrue(txnNames.contains(txnName2));

		Map<String, TracedMetricData> metrics1 = introspector.getMetricsForTransaction(txnName1);
		assertTrue(metrics1.keySet().contains(consume));
		
		
		Map<String, TracedMetricData> metrics2 = introspector.getMetricsForTransaction(txnName2);
		assertTrue(metrics2.keySet().contains(publish));
	}
	
	@Trace(dispatcher=true)
	public void subscribe(Mqtt5BlockingClient client, CompletableFuture<Boolean> canSend) {
		System.out.println("Call to subscribe");
		
		Mqtt5ConnAck conAck = client.connect();
		System.out.println("Sub Connected: "+conAck);
		
		Mqtt5SubAck subAck = client.subscribeWith().topicFilter(syncTopic).qos(MqttQos.EXACTLY_ONCE).send();
		System.out.println("Subscribe Ack: "+subAck);
		final Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
		canSend.complete(true);
		
		try 
	    {
			Optional<Mqtt5Publish> option = publishes.receive(10, TimeUnit.SECONDS);
			processIncoming(option,"first");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.disconnect();
		}
	}

	@Trace
	public void processIncoming(Optional<Mqtt5Publish> option,String messageNumber) {
		if(option.isPresent()) {
			Mqtt5Publish pub = option.get();
			System.out.println("Message from topic "+pub.getTopic().toString());
			System.out.println(messageNumber+" message received: "+new String(pub.getPayloadAsBytes()));
			System.out.println("message user properties: "+pub.getUserProperties());
		} else {
			System.out.println("no "+messageNumber+" message received");
		}
		
	}


	@Trace(dispatcher=true)
	public void publish(Mqtt5BlockingClient client, String payload, CompletableFuture<Boolean> ready) {
		System.out.println("Call to publish("+payload+")");
		client.connect();
		Integer i = null;
		
		try {
			i = Integer.parseInt(payload);
		} catch (NumberFormatException e) {
			i = Integer.valueOf(-1);
		}
		MqttUserPropertyImpl property = MqttUserPropertyImpl.of("MessageCount", Integer.toString(i));
		
		ImmutableList<MqttUserPropertyImpl> propList = ImmutableList.of(property);
		MqttUserPropertiesImpl props = MqttUserPropertiesImpl.of(propList);
		Boolean canSend = false;
		
		try {
			canSend = ready.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		if(canSend) {
			client.publishWith().topic(syncTopic).qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).userProperties(props).send();
		}
		client.disconnect();
	}


	private class SyncSubscriberThread extends Thread {

		Mqtt5BlockingClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking();
		CompletableFuture<Boolean> done = null;
		private CompletableFuture<Boolean> canSend = null;

		public SyncSubscriberThread(CompletableFuture<Boolean> d, CompletableFuture<Boolean> c) {
			super("SubscriberThread");
			done = d;
			canSend = c;
		}

		@Override
		public void run() {
				subscribe(client,canSend);
				done.complete(true);
		}

	}

	private class SyncPublisherThread extends Thread {

		int count = 0;
		CompletableFuture<Boolean> done = null;
		private CompletableFuture<Boolean> canSend = null;
		
		Mqtt5BlockingClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking();

		public SyncPublisherThread(CompletableFuture<Boolean> d,CompletableFuture<Boolean> c) {
			super("PublisherThread");
			done = d;
			canSend = c;
		}

		@Override
		public void run() {
				publish(client,Integer.toString(count),canSend);
				done.complete(true);
		}

	}

}
