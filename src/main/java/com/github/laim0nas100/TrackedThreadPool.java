package com.github.laim0nas100;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 * @author laim0nas100
 */
public class TrackedThreadPool extends ThreadGroup implements ThreadFactory {

    /**
     * Thread subclass to differentiate threads from the same group created by
     * this ThreadFactoryGroup
     */
    public static class TrackedThread extends Thread {

        protected final Runnable task;
        protected final TrackedThreadPool pool;

        public TrackedThread(TrackedThreadPool group, Runnable target, String name) {
            super(group, target, name);
            this.pool = Objects.requireNonNull(group);
            this.task = target;
        }

        public TrackedThread(TrackedThreadPool group, Runnable target, String name, boolean deamon, int priority, ClassLoader clLoader) {
            super(group, target, name);
            this.pool = Objects.requireNonNull(group);
            this.task = target;
            setDaemon(deamon);
            setPriority(priority);
            if (clLoader != null) {
                setContextClassLoader(clLoader);
            }
        }

        public Runnable getTask() {
            return task;
        }

        public TrackedThreadPool getPool() {
            return pool;
        }

        @Override
        public void run() {
            if (task == null) {//short lived, don't bother with counter
                return;
            }
            try {
                pool.threadsAliveCount.incrementAndGet();
                task.run();
            } finally {
                pool.threadsAliveCount.decrementAndGet();
            }
        }

    }

    protected boolean threadsDeamon = true;
    protected int threadsPriority = Thread.NORM_PRIORITY;
    protected boolean threadsStarting = false;
    protected String threadsPrefix = "";
    protected String threadsSuffix = "";
    protected AtomicLong threadsNum = new AtomicLong(1L);
    protected AtomicInteger threadsAliveCount = new AtomicInteger(0);
    protected ClassLoader loader;

    public TrackedThreadPool(ThreadGroup parent, String name) {
        super(parent, name);
    }

    public TrackedThreadPool(String name) {
        super(name);
    }

    public int getThreadsPriority() {
        return threadsPriority;
    }

    public void setThreadsPriority(int threadsPriority) {
        if (threadsPriority < Thread.MIN_PRIORITY || threadsPriority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException("Priority range is [1;10], your argument is:" + threadsPriority);
        }
        boolean change = this.threadsPriority != threadsPriority;
        this.threadsPriority = threadsPriority;
        if (change) {
            decorateAliveThreads(thread -> thread.setPriority(threadsPriority));
        }
    }

    public boolean isThreadsDeamon() {
        return threadsDeamon;
    }

    public void setThreadsDeamon(boolean deamon) {
        boolean change = deamon != this.threadsDeamon;
        this.threadsDeamon = deamon;
        if (change) {
            decorateAliveThreads(thread -> thread.setDaemon(deamon));
        }
    }

    public boolean isThreadsStarting() {
        return threadsStarting;
    }

    public void setThreadsStarting(boolean start) {
        this.threadsStarting = start;
    }

    public String getThreadsPrefix() {
        return threadsPrefix;
    }

    public void setThreadsPrefix(String threadsPrefix) {
        this.threadsPrefix = Objects.requireNonNull(threadsPrefix, "threadPrefix must not null");
    }

    public String getThreadsSuffix() {
        return threadsSuffix;
    }

    public void setThreadsSuffix(String threadsSuffix) {
        this.threadsSuffix = Objects.requireNonNull(threadsSuffix, "threadSuffix must not null");
    }

    protected void decorateAliveThreads(Consumer<Thread> consumer) {
        enumerate(false).filter(t -> t.isAlive()).forEach(consumer);
    }

    public Stream<Thread> enumerate(boolean recurse) {
        int activeCount = recurse ? activeCount() : threadsAliveCount.get();
        int padding = Math.max(8, activeCount / 4);// ensure none are ignored
        Thread[] threads = new Thread[activeCount + padding];
        enumerate(threads, recurse);
        return Stream.of(threads).filter(t -> threadEnumerationFilter(recurse, t));
    }

    protected String nextThreadName() {
        return threadsPrefix + threadsNum.getAndIncrement() + threadsSuffix;
    }

    protected boolean threadEnumerationFilter(boolean recurse, Thread t) {
        if (t instanceof TrackedThread) {
            TrackedThread st = (TrackedThread) t;
            return recurse || st.pool == this;
        }
        return false;
    }

    @Override
    public TrackedThread newThread(Runnable run) {
        Objects.requireNonNull(run, "Provided Runnable is null");
        TrackedThread thread = new TrackedThread(
                this,
                run,
                nextThreadName(),
                isThreadsDeamon(),
                getThreadsPriority(),
                getContextClassLoader()
        );
        if (isThreadsStarting()) {
            thread.start();
        }
        return thread;
    }

    public ClassLoader getContextClassLoader() {
        return loader;
    }

    public void setContextClassLoader(ClassLoader loader) {
        boolean change = this.loader != loader;
        this.loader = loader;
        if (change) {
            decorateAliveThreads(thread -> {
                thread.setContextClassLoader(loader);
            });
        }
    }

}
