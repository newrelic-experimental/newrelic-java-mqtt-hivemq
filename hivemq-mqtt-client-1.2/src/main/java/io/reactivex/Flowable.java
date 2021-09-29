package io.reactivex;

import java.util.logging.Level;

import com.newrelic.api.agent.NewRelic;
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
		 String onNextClass = onNext != null ? onNext.getClass().getName() : "null";
		 String onErrorClass = onError != null ? onError.getClass().getName() : "null";
		 String onCompleteClass = onComplete != null ? onComplete.getClass().getName() : "null";
		 String onAfterTerminateClass = onAfterTerminate != null ? onAfterTerminate.getClass().getName() : "null";
		 
		 NewRelic.getAgent().getLogger().log(Level.FINE, "Call to Flowable.doOnEach({0},{1},{2},{3})", onNextClass, onErrorClass, onCompleteClass, onAfterTerminateClass );
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
