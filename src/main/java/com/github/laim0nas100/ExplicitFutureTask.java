package com.github.laim0nas100;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author laim0nas100
 */
public class ExplicitFutureTask<T> extends FutureTask<T> {
    //FutureTask deesn't expose the state variable. the state() method is also insufficient

    protected final AtomicBoolean NEW = new AtomicBoolean(true);

    public ExplicitFutureTask(Callable<T> callable) {
        super(callable);
    }

    public ExplicitFutureTask(Runnable runnable, T result) {
        super(runnable, result);
    }

    @Override
    public void setException(Throwable t) {
        super.setException(t);
    }

    @Override
    public void run() {
        if (NEW.compareAndSet(true, false)) {
            super.run();
        }

    }

    public boolean isNew() {
        return NEW.get();
    }
}
