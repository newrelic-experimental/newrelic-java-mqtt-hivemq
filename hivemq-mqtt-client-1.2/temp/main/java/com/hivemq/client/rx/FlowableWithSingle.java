package com.hivemq.client.rx;

import java.util.concurrent.CompletableFuture;

import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.NRConsumerWapper;

import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;

@Weave
public abstract class FlowableWithSingle<F, S> {

	public CompletableFuture<S> subscribeSingleFuture(Consumer<? super F> onNext) {
		if(!(onNext instanceof NRConsumerWapper)) {
			NRConsumerWapper<? super F> wrapper = new NRConsumerWapper<F>(onNext);
			onNext = (Consumer<? super F>) wrapper;
		}
		return Weaver.callOriginal();
	}
	
	public CompletableFuture<S> subscribeSingleFuture(Consumer<? super F> onNext, Consumer<? super Throwable> onError) {
		if(!(onNext instanceof NRConsumerWapper)) {
			NRConsumerWapper<? super F> wrapper = new NRConsumerWapper<F>(onNext);
			onNext = (Consumer<? super F>) wrapper;
		}
		return Weaver.callOriginal();
	}
	
	public CompletableFuture<S> subscribeSingleFuture(Consumer<? super F> onNext, Consumer<? super Throwable> onError, Action onComplete) {
		if(!(onNext instanceof NRConsumerWapper)) {
			NRConsumerWapper<? super F> wrapper = new NRConsumerWapper<F>(onNext);
			onNext = (Consumer<? super F>) wrapper;
		}
		return Weaver.callOriginal();
	}
}
