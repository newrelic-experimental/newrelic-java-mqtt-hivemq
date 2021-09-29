package com.nr.instrumentation.test.hivemq;

import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.agent.introspec.Introspector;
import com.newrelic.agent.introspec.TraceSegment;
import com.newrelic.agent.introspec.TracedMetricData;
import com.newrelic.api.agent.Trace;

@RunWith(InstrumentationTestRunner.class)
@InstrumentationTestConfig(includePrefixes="com.hivemq.client")
public class TestMQTT5AsyncClient {

	static int max = 5;
	static EmbeddedHiveMQ hiveMQ = null;
	static final String hivehost = "localhost";
	static final String asyncTopic = "newrelic/test-async";

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
	public void doAsyncPubSubTest() {

		String txnName1 = "OtherTransaction/Custom/com.nr.instrumentation.test.hivemq.TestMQTT5AsyncClient/subscribe";
		String txnName2 = "OtherTransaction/Custom/com.nr.instrumentation.test.hivemq.TestMQTT5AsyncClient/publish";
		String consume = "MessageBroker/HiveMQ/Topic/Consume/Named/newrelic_test-async";
		String publish = "MessageBroker/HiveMQ/Topic/Produce/Named/newrelic_test-async";

		CompletableFuture<Boolean> pubDone = new CompletableFuture<Boolean>();
		CompletableFuture<Boolean> subDone = new CompletableFuture<Boolean>();
		CompletableFuture<Boolean> readyToSend = new CompletableFuture<Boolean>();

		AsyncPublisherThread pThread = new AsyncPublisherThread(pubDone, readyToSend);
		AsyncSubscriberThread sThread = new AsyncSubscriberThread(subDone, readyToSend);

		sThread.start();
		pThread.start();

		try {
			Boolean doneWithSub = subDone.get(20, TimeUnit.SECONDS);
			System.out.println("doneWithSub = "+doneWithSub);
			Boolean doneWithPub = pubDone.get(20, TimeUnit.SECONDS);
			System.out.println("doneWithPub = "+doneWithPub);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(15000);
		System.out.println("Finished transaction count: "+finishedTransactionCount);
		//assertTrue(finishedTransactionCount == 4);
		Collection<String> txnNames = introspector.getTransactionNames();
		for(String tName : txnNames) {
			System.out.println("Transaction Name: "+tName);
		}
		assertTrue(txnNames.contains(txnName1));
		assertTrue(txnNames.contains(txnName2));

		Map<String, TracedMetricData> metrics1 = introspector.getMetricsForTransaction(txnName1);
		assertTrue(metrics1.keySet().contains(consume));

		Map<String, TracedMetricData> metrics2 = introspector.getMetricsForTransaction(txnName2);
		assertTrue(metrics2.keySet().contains(publish));


		introspector.clear();
		System.out.println("AsyncPubSubTest passed");

	}

	@Trace(dispatcher=true)
	public void subscribe(Mqtt5AsyncClient client, CompletableFuture<Boolean> ready) {
		final Listener listener = new Listener();

		try {

			CompletableFuture<Mqtt5ConnAck> f = client.connect();
			Mqtt5ConnAck ack = f.get();
			System.out.println("Connected: "+ack);

			CompletableFuture<Mqtt5SubAck> f2 = client.subscribeWith().topicFilter(asyncTopic).qos(MqttQos.EXACTLY_ONCE).send();

			f2.get();

			Consumer<Mqtt5Publish> consumer = new Consumer<Mqtt5Publish>() {

				@Override
				public void accept(Mqtt5Publish publish) {
					System.out.println("Received message from "+publish.getTopic()+ ", payload: "+new String(publish.getPayloadAsBytes()));
					listener.setDone();
				}
			};


			client.publishes(MqttGlobalPublishFilter.ALL,consumer);
			ready.complete(true);

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		while(!listener.isDone()) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
		}
		try {
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Trace(dispatcher=true)
	public void publish(Mqtt5AsyncClient client, String payload, CompletableFuture<Boolean> ready) {
		System.out.println("Call to publish("+payload+")");
		try {
			client.connect().get();
			Boolean canSend = ready.get();
			if(canSend) {
				CompletableFuture<Mqtt5PublishResult> pubResult = client.publishWith().topic(asyncTopic).qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).send();
				Mqtt5PublishResult result = pubResult.get();
				System.out.println("Publish Result: "+result);
			}
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void printSegments(List<TraceSegment> children, int indent) {
		for(TraceSegment child : children) {
			printSegment(child, indent);
		}

	}

	private static void printSegment(TraceSegment segment, int indent) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indent;i++) {
			sb.append('\t');
		}
		String prefix = sb.toString();
		System.out.println(prefix + "Segment: "+segment.getName());
		System.out.println(prefix + "\tCall Count: "+segment.getCallCount());
		System.out.println(prefix + "\tClass Name: "+segment.getClassName());
		List<TraceSegment> children = segment.getChildren();
		printSegments(children, indent+1);
	}

	private class AsyncSubscriberThread extends Thread {

		private CompletableFuture<Boolean> done = null;
		private CompletableFuture<Boolean> canSend = null;

		Mqtt5AsyncClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking().toAsync();

		public AsyncSubscriberThread(CompletableFuture<Boolean> d,CompletableFuture<Boolean> c) {
			super("SubscriberThread");
			done = d;
			canSend = c;
		}

		@Override
		public void run() {
			subscribe(client, canSend);
			pause(100L);
			done.complete(true);
		}

		private void pause(long ms) {
			if(ms > 0) {
				try {
					sleep(ms);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private class Listener {

		boolean done;

		Listener() {
			done = false;
		}

		public void setDone() {
			done = true;
		}

		public boolean isDone() {
			return done;
		}
	}

	private class AsyncPublisherThread extends Thread {

		int count = 0;
		private CompletableFuture<Boolean> done = null;
		private CompletableFuture<Boolean> canSend = null;

		Mqtt5AsyncClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking()
				.toAsync();

		public AsyncPublisherThread(CompletableFuture<Boolean> d,CompletableFuture<Boolean> c) {
			super("PublisherThread");
			done = d;
			canSend = c;
		}

		@Override
		public void run() {
			publish(client,Integer.toString(count),canSend);
			pause(200L);
			done.complete(true);
		}

		private void pause(long ms) {
			if(ms > 0) {
				try {
					sleep(ms);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
