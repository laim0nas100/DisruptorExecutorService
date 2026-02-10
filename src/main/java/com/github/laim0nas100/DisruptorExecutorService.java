package com.github.laim0nas100;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author laim0nas100
 */
public class DisruptorExecutorService implements ExecutorService {

    public static final class TaskEvent {

        public FutureTask<?> task;

        public ExplicitFutureTask getClearIfExplicit() {
            FutureTask t = task;
            if (t instanceof ExplicitFutureTask) {
                ExplicitFutureTask r = (ExplicitFutureTask) t;
                task = null;
                return r;
            }
            return null;

        }

        public FutureTask getClear() {
            FutureTask r = task;
            task = null;
            return r;
        }
    }

    public static class Worker implements Runnable {

        protected final DisruptorExecutorService service;
        protected final SequenceBarrier barrier;
        protected final Sequence sequence;
        protected boolean markedForDeath;
        protected Thread runner;

        public Worker(DisruptorExecutorService service) {
            this.service = Objects.requireNonNull(service);
            this.barrier = service.ringBuffer.newBarrier();
            this.sequence = new Sequence();
        }

        @Override
        public void run() {
            this.runner = Thread.currentThread();
            sequence.set(service.sharedGatingSequence.get());
            try {
                while (!service.halted && !markedForDeath) {
                    try {
                        long seq = sequence.get();
                        long available = barrier.waitFor(seq);// overlap, if some slots are race-skiped
                        while (seq <= available) {
                            TaskEvent event = service.ringBuffer.get(seq);

                            // Claim this slot - only one worker wins
                            if (service.sharedGatingSequence.compareAndSet(seq - 1, seq)) {
                                FutureTask task = event.getClear();
                                if (task != null) {
                                    task.run();
                                }
                                seq++;
                            } else {
                                LockSupport.parkNanos(1);//CAS contention backoff 
                                barrier.checkAlert();
                                seq = Math.max(service.sharedGatingSequence.get() - 1, seq + 1);// potential sequence skip
                            }

                        }
                        sequence.set(available);

                        //don't end the thread just because of timeout or interrupt
                    } catch (InterruptedException | com.lmax.disruptor.TimeoutException ignored) {
                        barrier.checkAlert();
                    }
                }
            } catch (AlertException e) {
                // Normal shutdown path
            } finally {
                try {

                    service.workersLock.lock();
                    service.workers.remove(this);
                    // Last worker completes the future
                    if (service.halted && service.workers.isEmpty()) {
                        service.awaitFuture.complete(0);
                    }
                } finally {
                    service.workersLock.unlock();
                }

            }
        }

        @Override
        public String toString() {
            return "Worker{" + "sequence=" + sequence + ", markedForDeath=" + markedForDeath + ", runner=" + runner + '}';
        }

    }

    protected final EventTranslatorOneArg<TaskEvent, Runnable> translator = new EventTranslatorOneArg<>() {
        @Override
        public void translateTo(TaskEvent event, long sequence, Runnable run) {
            ExplicitFutureTask<?> task = event.getClearIfExplicit();
            if (task != null) {
                //potentionally overflowed slot. execute in place
                task.run();
            }
            if (run != null) {
                event.task = (FutureTask) newTaskFor(run, null);
            }

        }
    };

    /**
     * DEFAULTS
     */
    public static final int DEFAULT_BUFFER_SIZE = 65536;
    public static final ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.SINGLE;
    public static final WaitStrategy DEFAULT_WAIT_STRATEGY = new BlockingWaitStrategy();

    public static final int MIN_BUFFER_SIZE = 512;

    protected final AtomicBoolean open = new AtomicBoolean(true);
    protected volatile boolean halted = false;

    protected final CompletableFuture awaitFuture = new CompletableFuture();

    protected final TrackedThreadPool pool;
    protected final Disruptor<TaskEvent> disruptor;
    protected final RingBuffer<TaskEvent> ringBuffer;
    protected final Sequence sharedGatingSequence = new Sequence();
    protected final int bufferLimitForPublishAttempt;
    protected final int batchSize;

    //user workersLock when interacting with this set
    protected final Set<Worker> workers = new HashSet<>();
    protected final ReentrantLock workersLock = new ReentrantLock();

    public DisruptorExecutorService() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public DisruptorExecutorService(int bufferSize) {
        this(bufferSize, DEFAULT_PRODUCER_TYPE, DEFAULT_WAIT_STRATEGY);
    }

    public DisruptorExecutorService(int bufferSize, ProducerType producer, WaitStrategy strategy) {

        if (bufferSize < MIN_BUFFER_SIZE || (bufferSize != Integer.highestOneBit(bufferSize))) {
            throw new IllegalArgumentException("buffer size must be at least " + bufferSize + " and a power of 2");
        }
        pool = new TrackedThreadPool("DisruptorExe");
        pool.setThreadsPrefix("DisruptorExeThread-");
        pool.setThreadsStarting(false);
        pool.setThreadsDeamon(true);
        bufferLimitForPublishAttempt = Math.max(bufferSize / 512, 128);
        batchSize = bufferSize / 4;
        disruptor = new Disruptor<>(TaskEvent::new, bufferSize, pool, producer, strategy);
        ringBuffer = disruptor.getRingBuffer();
        ringBuffer.addGatingSequences(sharedGatingSequence);
        disruptor.start();

    }

    /**
     * Can grow or shrink pool size. Setting to 0 effectively stops the workers
     * without shutting down the executor.
     *
     * @param poolSize desired pool size
     * @return the worker change, negative or positive
     */
    public int ensurePoolSize(int poolSize) {
        int grew = 0;
        try {
            workersLock.lock();

            int size = (int) workers.stream()
                    .filter(worker -> !worker.markedForDeath)
                    .count();
            //shrink threads gracefully
            if (size > poolSize) {
                return (int) workers.stream()
                        .filter(worker -> !worker.markedForDeath)
                        .limit(size - poolSize)
                        .peek(worker -> {
                            worker.markedForDeath = true;
                            worker.barrier.alert();
                        }).count() * -1;
            }

            for (int i = size; i < poolSize; i++) {
                Worker worker = new Worker(this);
                workers.add(worker);
                pool.newThread(worker).start();
                grew++;
            }
            return grew;
        } finally {
            workersLock.unlock();
        }

    }

    @Override
    public boolean isShutdown() {
        return !open.get();
    }

    @Override
    public void shutdown() {//gracefull shutdown
        if (!open.compareAndSet(true, false)) {
            throw new IllegalStateException("Executor was already shut down");
        }

        if (pool.activeCount() == 0) {
            halted = true;
            awaitFuture.complete(0);
            return;
        }
        //poison pill shutdown
        disruptor.getRingBuffer().publishEvent((TaskEvent event, long sequence) -> {
            event.task = new FutureTask<>(() -> {
                halted = true;
                try {
                    workersLock.lock();
                    workers.forEach(worker -> {
                        worker.barrier.alert();
                    });
                } finally {
                    workersLock.unlock();
                }

                return 0;
            });
        });

    }

    @Override
    public List<Runnable> shutdownNow() {//abrupt shutdown
        if (!open.compareAndSet(true, false)) {
            throw new IllegalStateException("Executor was already shut down");
        }
        halted = true;

        List<Runnable> left = new ArrayList<>();

        long size = ringBuffer.getBufferSize();
        //don't care about already finished, just skip it. The idea is to scan the whole buffer
        long offset = ringBuffer.getMinimumGatingSequence();
        for (long i = 0; i < size; i++) {
            TaskEvent get = ringBuffer.get(offset + i);
            if (get == null || get.task == null) {
                continue;
            }
            ExplicitFutureTask task = get.getClearIfExplicit();
            if (task != null && task.isNew()) {
                left.add(task);
            }
        }
        if (pool.activeCount() == 0) {
            awaitFuture.complete(0);
            return left;
        }
        try {
            workersLock.lock();
            workers.forEach(worker -> {
                worker.barrier.alert();
            });
        } finally {
            workersLock.unlock();
        }
        return left;
    }

    @Override
    public boolean isTerminated() {
        return awaitFuture.isDone();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            awaitFuture.get(timeout, unit);
            return true;
        } catch (ExecutionException | TimeoutException ex) {
            return false;
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown()) {
            throw new IllegalStateException("Executor is shut down");
        }
        if (ringBuffer.remainingCapacity() >= bufferLimitForPublishAttempt) {
            ringBuffer.publishEvent(translator, command);
        } else {
            command.run();
        }

    }

    protected <T> ExplicitFutureTask<T> newTaskFor(Callable<T> task) {
        if (task instanceof ExplicitFutureTask) {
            return (ExplicitFutureTask) task;
        } else {
            return new ExplicitFutureTask<>(task);
        }
    }

    protected <T> ExplicitFutureTask<T> newTaskFor(Runnable task, T res) {
        if (task instanceof ExplicitFutureTask) {
            return (ExplicitFutureTask) task;
        } else {
            return new ExplicitFutureTask<>(task, res);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        ExplicitFutureTask<T> t = newTaskFor(task);
        execute(t);
        return t;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        ExplicitFutureTask<T> t = newTaskFor(task, result);
        execute(t);
        return t;
    }

    @Override
    public Future<?> submit(Runnable task) {
        ExplicitFutureTask<?> t = newTaskFor(task, null);
        execute(t);
        return t;
    }

    public void executeAll(Collection<Runnable> all) {
        if (isShutdown()) {
            throw new IllegalStateException("Executor is shut down");
        }

        int bufferSize = ringBuffer.getBufferSize();
        
        int allSize = all.size();
        if (allSize <= batchSize) {
            ringBuffer.publishEvents(translator, all.toArray(Runnable[]::new));
        } else {
            List<Runnable> list;
            if (all instanceof List && all instanceof RandomAccess) {
                list = (List) all;
            } else {
                list = new ArrayList<>(all);
            }

            int batch = 0;
            for (;;) {
                int from = batch * batchSize;
                int inc = Math.min(batchSize, allSize - from);
                int to = from + inc;
                int araySize = to - from;
                batch++;

                Runnable[] array = new Runnable[araySize];
                ringBuffer.publishEvents(translator, list.subList(from, to).toArray(array));
                if (inc < batchSize || allSize <= to) {
                    break;
                }
            }
        }
    }

    /**
     * the main mechanics of invokeAny.
     */
    private <T> T doInvokeAny(Collection<? extends Callable<T>> tasks,
            boolean timed, long nanos)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (tasks == null) {
            throw new NullPointerException();
        }
        int ntasks = tasks.size();
        if (ntasks == 0) {
            throw new IllegalArgumentException();
        }
        ArrayList<Future<T>> futures = new ArrayList<>(ntasks);
        ExecutorCompletionService<T> ecs
                = new ExecutorCompletionService<>(this);

        // For efficiency, especially in executors with limited
        // parallelism, check to see if previously submitted tasks are
        // done before submitting more of them. This interleaving
        // plus the exception mechanics account for messiness of main
        // loop.
        try {
            // Record exceptions so that if we fail to obtain any
            // result, we can throw the last exception we got.
            ExecutionException ee = null;
            final long deadline = timed ? System.nanoTime() + nanos : 0L;
            Iterator<? extends Callable<T>> it = tasks.iterator();

            // Start one task for sure; the rest incrementally
            futures.add(ecs.submit(it.next()));
            --ntasks;
            int active = 1;

            for (;;) {
                Future<T> f = ecs.poll();
                if (f == null) {
                    if (ntasks > 0) {
                        --ntasks;
                        futures.add(ecs.submit(it.next()));
                        ++active;
                    } else if (active == 0) {
                        break;
                    } else if (timed) {
                        f = ecs.poll(nanos, TimeUnit.NANOSECONDS);
                        if (f == null) {
                            throw new TimeoutException();
                        }
                        nanos = deadline - System.nanoTime();
                    } else {
                        f = ecs.take();
                    }
                }
                if (f != null) {
                    --active;
                    try {
                        return f.get();
                    } catch (ExecutionException eex) {
                        ee = eex;
                    } catch (RuntimeException rex) {
                        ee = new ExecutionException(rex);
                    }
                }
            }

            if (ee == null) {
                ee = new ExecutionException(new IllegalStateException("Failed without exception"));
            }
            throw ee;

        } finally {
            cancelAll(futures);
        }
    }

    /**
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws ExecutionException {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        try {
            return doInvokeAny(tasks, false, 0);
        } catch (TimeoutException cannotHappen) {
            assert false;
            return null;
        }
    }

    /**
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * @throws TimeoutException {@inheritDoc}
     * @throws ExecutionException {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
            long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return doInvokeAny(tasks, true, unit.toNanos(timeout));
    }

    /**
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        if (tasks == null) {
            throw new NullPointerException();
        }
        ArrayList<RunnableFuture<T>> futures = new ArrayList<>(tasks.size());
        try {
            for (Callable<T> t : tasks) {
                RunnableFuture<T> f = newTaskFor(t);
                futures.add(f);
            }
            executeAll((Collection) futures);
            for (int i = 0, size = futures.size(); i < size; i++) {
                Future<T> f = futures.get(i);
                if (!f.isDone()) {
                    try {
                        f.get();
                    } catch (CancellationException | ExecutionException ignore) {
                    }
                }
            }
            return (List) futures;
        } catch (Throwable t) {
            cancelAll((ArrayList) futures);
            throw t;
        }
    }

    /**
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
            long timeout, TimeUnit unit)
            throws InterruptedException {
        if (tasks == null) {
            throw new NullPointerException();
        }
        final long nanos = unit.toNanos(timeout);
        final long deadline = System.nanoTime() + nanos;
        ArrayList<Future<T>> futures = new ArrayList<>(tasks.size());
        int j = 0;
        timedOut:
        try {
            for (Callable<T> t : tasks) {
                futures.add(newTaskFor(t));
            }

            final int size = futures.size();

            executeAll((List) futures);

            for (; j < size; j++) {
                Future<T> f = futures.get(j);
                if (!f.isDone()) {
                    try {
                        f.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                    } catch (CancellationException | ExecutionException ignore) {
                    } catch (TimeoutException timedOut) {
                        break timedOut;
                    }
                }
            }
            return futures;
        } catch (Throwable t) {
            cancelAll(futures);
            throw t;
        }
        // Timed out before all the tasks could be completed; cancel remaining
        cancelAll(futures, j);
        return futures;
    }

    protected <T> void cancelAll(List<Future<T>> futures) {
        cancelAll(futures, 0);
    }

    /**
     * Cancels all futures with index at least j.
     */
    protected <T> void cancelAll(List<Future<T>> futures, int j) {
        for (int size = futures.size(); j < size; j++) {
            futures.get(j).cancel(true);
        }
    }
}
