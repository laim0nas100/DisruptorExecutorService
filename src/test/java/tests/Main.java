package tests;

import com.github.laim0nas100.DisruptorExecutorService;
import com.github.laim0nas100.DisruptorExecutorService.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author laim0nas100
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        DisruptorExecutorService service = new DisruptorExecutorService(1024 * 256);
        service.ensurePoolSize(5);
//        ExecutorService service = Executors.newFixedThreadPool(10);

        int size = 10000000;
        boolean bunch = false;
        boolean shutdownNow = false;

        AtomicInteger atomic = new AtomicInteger(0);

        if (bunch) {
            List<Callable<Integer>> list = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                if (i == 100000) {
                    service.ensurePoolSize(10);
                }
                list.add(() -> {
                    for (int j = 0; j < size * 100; j++) {
                    }
                    return atomic.incrementAndGet();
//                System.out.println(Thread.currentThread().getName() +" "+ n);
                });
            }
            service.invokeAll(list);
        } else {
            for (int i = 0; i < size; i++) {
                if (i == 100000) {
                    service.ensurePoolSize(10);
                }
                service.submit(() -> {
                    for (int j = 0; j < size * 100; j++) {
                    }
                    return atomic.incrementAndGet();
//                System.out.println(Thread.currentThread().getName() +" "+ n);
                });
            }
        }
        System.out.println(atomic.get());

        if (shutdownNow) {
            List<Runnable> leftRunnables = service.shutdownNow();
            service.awaitTermination(1, TimeUnit.DAYS);
            System.out.println(atomic.get() + leftRunnables.stream().map(m -> (Task) m).filter(f -> f.isNew()).count());
        } else {
            service.shutdown();
            service.awaitTermination(1, TimeUnit.DAYS);
            System.out.println(atomic.get());
        }
    }

}
