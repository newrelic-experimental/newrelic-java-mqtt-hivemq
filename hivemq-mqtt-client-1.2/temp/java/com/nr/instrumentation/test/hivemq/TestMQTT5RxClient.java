package com.nr.instrumentation.test.hivemq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.api.agent.Trace;

import io.reactivex.Flowable;
import io.reactivex.Single;

@RunWith(InstrumentationTestRunner.class)
@InstrumentationTestConfig(includePrefixes="com.hivemq.client")
public class TestMQTT5RxClient {

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

	@Test
	public void testSubscribe() {
		CompletableFuture<Boolean> subDone = new CompletableFuture<Boolean>();
		RxSubscriberThread sThread = new RxSubscriberThread(subDone);
		sThread.start();
		
		CompletableFuture<Boolean> pubDone = new CompletableFuture<Boolean>();
		
		RxPublisherThread pThread = new RxPublisherThread(pubDone);
		pThread.start();

//		try {
//			Boolean isSubDone = subDone.get(10L, TimeUnit.SECONDS);
//			Boolean isPubDone = pubDone.get(10L, TimeUnit.SECONDS);
//			System.out.println("SubDone: "+isSubDone);
//			System.out.println("PubDone: "+isPubDone);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (ExecutionException e) {
//			e.printStackTrace();
//		} catch (TimeoutException e) {
//			e.printStackTrace();
//		}

	}

	@Trace(dispatcher=true)
	public void publish(Mqtt5RxClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
		client.connect().blockingGet();
		Mqtt5Publish pub = Mqtt5Publish.builder().topic("test/nrdoug-rx").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).build();

		Single<Mqtt5PublishResult> single = client.publish(Flowable.just(pub)).singleOrError();
		Mqtt5PublishResult pubResult = single.blockingGet();

		System.out.println("Publish Result "+pubResult);
		client.disconnect().blockingAwait();
	}



	@Trace(dispatcher=true)
	public void processMessage(Mqtt5Publish publish) {
		System.out.println("Received message from "+publish.getTopic()+ ", payload: "+new String(publish.getPayloadAsBytes()));
	}


	public CompletableFuture<Boolean> subscribe(Mqtt5RxClient client) {

		CompletableFuture<Boolean> done = new CompletableFuture<Boolean>();

		Single<Mqtt5ConnAck> f = client.connect();
		Mqtt5ConnAck ack = f.blockingGet();
		System.out.println("Connected: "+ack);

		Single<Mqtt5SubAck> f2 = client.subscribeWith().topicFilter("test/nrdoug-rx").qos(MqttQos.EXACTLY_ONCE).applySubscribe();

		Mqtt5SubAck subAck = f2.blockingGet();
		System.out.println("subscribe ack: "+subAck);

		Flowable<Mqtt5Publish> f3 = client.publishes(MqttGlobalPublishFilter.ALL);

		f3 = f3.doOnNext(publish -> {
				processMessage(publish);
				done.complete(true);
			}
		);

		return done;
	}



	private class RxSubscriberThread extends Thread {


		Mqtt5RxClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking().toRx();

		CompletableFuture<Boolean> done = null;

		public RxSubscriberThread(CompletableFuture<Boolean> d) {
			super("SubscriberThread");
			done = d;
		}

		@Override
		public void run() {
			CompletableFuture<Boolean> msgReceived = subscribe(client);
			Boolean b = null;

			try {
				b = msgReceived.get(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
			if(b != null) {
				done.complete(b);
			} else {
				done.complete(true);
			}
			client.disconnect().blockingAwait();
		}

	}

	private class RxPublisherThread extends Thread {

		int count = 0;
		CompletableFuture<Boolean> done = null;

		Mqtt5RxClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking().toRx();
		
		public RxPublisherThread(CompletableFuture<Boolean> d) {
			super("PublisherThread");
			done = d;
		}

		@Override
		public void run() {
			publish(client,Integer.toString(count));
			done.complete(true);
		}

	}

}
