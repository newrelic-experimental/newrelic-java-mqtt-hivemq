package io.reactivex;

import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.nr.instrumentation.hivemq.client.NRConsumerWapper;

import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

@Weave
public abstract class Flowable<T> {
	
	public static <T> Flowable<T> just(T item) {
		return Weaver.callOriginal();
	}

	 @SuppressWarnings("unused")
	private Flowable<T> doOnEach(Consumer<? super T> onNext, Consumer<? super Throwable> onError,
	            Action onComplete, Action onAfterTerminate) {
		if (!(onNext instanceof NRConsumerWapper) /* && this instanceof MqttGlobalIncomingPublishFlowable */) {
			 NRConsumerWapper<T> wrapper = new NRConsumerWapper<T>(onNext);
			 onNext = wrapper;
		 }
		 return Weaver.callOriginal();
	 }
	 
	 public abstract <R> Flowable<R> map(Function<? super T, ? extends R> mapper);
	 public abstract Flowable<T> doOnNext(Consumer<? super T> onNext);
	 public abstract T blockingFirst();
	 public abstract Single<T> singleOrError();
}
