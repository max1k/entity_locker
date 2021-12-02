package ru.mxk.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class BaseEntityLockerTest {
    private static final int THREAD_LIMIT = 1000;
    private static final int FIRST_ENTITY_ID = 1;
    private static final int SECOND_ENTITY_ID = 42;
    private static final List<Integer> EXPECTED_LIST = IntStream.range(1, THREAD_LIMIT + 1).boxed().collect(Collectors.toList());

    @Test
    public void testAtMostOneExecutionOnSameEntity() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> mutualList = new ArrayList<>(THREAD_LIMIT);
        EntityLocker<Integer> entityLocker = new BaseEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();
                entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> mutualList.add(mutualList.size() + 1));
            } catch (InterruptedException e) {
                Assertions.fail();
            }
        };

        List<Thread> threads = Stream.generate(() -> new Thread(threadRunnable))
                                     .limit(THREAD_LIMIT)
                                     .peek(Thread::start)
                                     .collect(Collectors.toList());

        //Starting lockAndRun execution at the same time
        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(EXPECTED_LIST, mutualList);
    }

    @Test
    public void testReentrantLocking() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> firstMutualList = new ArrayList<>(THREAD_LIMIT);
        List<Integer> secondMutualList = new ArrayList<>(THREAD_LIMIT);
        EntityLocker<Integer> entityLocker = new BaseEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();

                entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> {
                    firstMutualList.add(firstMutualList.size() + 1);
                    entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> secondMutualList.add(secondMutualList.size() + 1));
                });

            } catch (InterruptedException e) {
                Assertions.fail();
            }
        };

        List<Thread> threads = Stream.generate(() -> new Thread(threadRunnable))
                                     .limit(THREAD_LIMIT)
                                     .peek(Thread::start)
                                     .collect(Collectors.toList());

        //Starting lockAndRun execution at the same time
        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(EXPECTED_LIST, firstMutualList);
        Assertions.assertEquals(EXPECTED_LIST, secondMutualList);
    }


    @Test
    public void testDeadLock() throws InterruptedException {
        EntityLocker<Integer> entityLocker = new BaseEntityLocker<>();
        List<Integer> firstList = new ArrayList<>(THREAD_LIMIT);
        List<Integer> secondList = new ArrayList<>(THREAD_LIMIT);
        CountDownLatch latch = new CountDownLatch(1);

        Runnable firstRunnable = () -> {
            try {
                latch.await();
                entityLocker.lockAndRun(
                        FIRST_ENTITY_ID,
                        () -> entityLocker.lockAndRun(SECOND_ENTITY_ID, () -> firstList.add(FIRST_ENTITY_ID))
                );
            } catch (InterruptedException e) {
                Assertions.fail();
            }

        };

        Runnable secondRunnable = () -> {
            try {
                latch.await();
                entityLocker.lockAndRun(
                        SECOND_ENTITY_ID,
                        () -> entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> secondList.add(SECOND_ENTITY_ID))
                );
            } catch (InterruptedException e) {
                Assertions.fail();
            }
        };

        List<Thread> threads = new ArrayList<>(THREAD_LIMIT);
        for (int i = 0; i < THREAD_LIMIT; i++) {
            Thread thread = new Thread(i % 2 == 0 ? firstRunnable : secondRunnable);
            thread.start();
            threads.add(thread);
        }

        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(firstList, secondList);
    }

}