package ru.mxk.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class ReentrantEntityLockerTest {
    private static final int THREAD_LIMIT = 1000;
    private static final int FIRST_ENTITY_ID = 1;
    private static final int SECOND_ENTITY_ID = 42;
    private static final int DEFAULT_SLEEP_TIME_MS = 500;
    private static final List<Integer> EXPECTED_LIST = IntStream.range(1, THREAD_LIMIT + 1).boxed().collect(Collectors.toList());

    @Test
    public void testAtMostOneExecutionOnSameEntity() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> mutualList = new ArrayList<>(THREAD_LIMIT);
        EntityLocker<Integer> entityLocker = new ReentrantEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();
                entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> mutualList.add(mutualList.size() + 1));
            } catch (InterruptedException e) {
                Assertions.fail(e);
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
        EntityLocker<Integer> entityLocker = new ReentrantEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();

                entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> {
                    firstMutualList.add(firstMutualList.size() + 1);
                    entityLocker.lockAndRun(FIRST_ENTITY_ID, () -> secondMutualList.add(secondMutualList.size() + 1));
                });

            } catch (InterruptedException e) {
                Assertions.fail(e);
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
    public void testConcurrentExecutionOnDifferentEntities() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        EntityLocker<UUID> entityLocker = new ReentrantEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();
                entityLocker.lockAndRun(UUID.randomUUID(), () -> {
                    counter.incrementAndGet();
                    try {
                        Thread.sleep(DEFAULT_SLEEP_TIME_MS);
                    } catch (InterruptedException e) {
                        Assertions.fail(e);
                    }
                });
            } catch (InterruptedException e) {
                Assertions.fail(e);
            }
        };

        long beginTime = System.currentTimeMillis();

        List<Thread> threads = Stream.generate(() -> new Thread(threadRunnable))
                                     .limit(THREAD_LIMIT)
                                     .peek(Thread::start)
                                     .collect(Collectors.toList());

        //Starting lockAndRun execution at the same time
        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        long endTime = System.currentTimeMillis();
        Duration runDuration = Duration.ofMillis(endTime - beginTime);

        Assertions.assertEquals(THREAD_LIMIT, counter.get());
        Assertions.assertTrue(runDuration.compareTo(Duration.ofMillis(DEFAULT_SLEEP_TIME_MS)) >= 0);
        Assertions.assertTrue(runDuration.compareTo(Duration.ofMillis(THREAD_LIMIT * DEFAULT_SLEEP_TIME_MS)) < 0);
    }

    @Test
    public void testLockWithTimeoutWaitingTimeElapses() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        List<Boolean> protectedCodeExecutions = new CopyOnWriteArrayList<>();
        EntityLocker<Integer> entityLocker = new ReentrantEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();
                boolean executed = entityLocker.lockAndRun(
                        FIRST_ENTITY_ID,
                        () -> {
                            counter.incrementAndGet();
                            try {
                                Thread.sleep(DEFAULT_SLEEP_TIME_MS);
                            } catch (InterruptedException e) {
                                Assertions.fail(e);
                            }
                        },
                        Duration.ofMillis(DEFAULT_SLEEP_TIME_MS / 2)
                );
                protectedCodeExecutions.add(executed);
            } catch (InterruptedException e) {
                Assertions.fail(e);
            }
        };

        //Starting lockAndRun execution at the same time
        latch.countDown();

        List<Thread> threads = Stream.generate(() -> new Thread(threadRunnable))
                                     .limit(THREAD_LIMIT)
                                     .peek(Thread::start)
                                     .collect(Collectors.toList());

        //Starting lockAndRun execution at the same time
        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(1, counter.get());
        Assertions.assertEquals(THREAD_LIMIT, protectedCodeExecutions.size());

        Map<Boolean, Long> executionsCount = protectedCodeExecutions
                .stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertEquals(1, executionsCount.get(true));
    }

    @Test
    public void testLockWithTimeoutWaitingTimeDoesNotElapse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        List<Boolean> protectedCodeExecutions = new CopyOnWriteArrayList<>();
        EntityLocker<Integer> entityLocker = new ReentrantEntityLocker<>();

        Runnable threadRunnable = () -> {
            try {
                latch.await();
                boolean executed = entityLocker.lockAndRun(
                        FIRST_ENTITY_ID,
                        () -> {
                            counter.incrementAndGet();
                            try {
                                Thread.sleep(DEFAULT_SLEEP_TIME_MS);
                            } catch (InterruptedException e) {
                                Assertions.fail(e);
                            }
                        },
                        Duration.ofMillis(DEFAULT_SLEEP_TIME_MS * 3 / 2)
                );
                protectedCodeExecutions.add(executed);
            } catch (InterruptedException e) {
                Assertions.fail(e);
            }
        };

        //Starting lockAndRun execution at the same time
        latch.countDown();

        List<Thread> threads = Stream.generate(() -> new Thread(threadRunnable))
                                     .limit(THREAD_LIMIT)
                                     .peek(Thread::start)
                                     .collect(Collectors.toList());

        //Starting lockAndRun execution at the same time
        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(2, counter.get());
        Assertions.assertEquals(THREAD_LIMIT, protectedCodeExecutions.size());

        Map<Boolean, Long> executionsCount = protectedCodeExecutions
                .stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertEquals(2, executionsCount.get(true));
    }

    @Test
    public void testAtMostOneExecutionOnGlobalLock() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> mutualList = new ArrayList<>(THREAD_LIMIT);


        Runnable threadRunnable = () -> {
            try {
                latch.await();
                EntityLocker.lockGlobalAndRun(() -> mutualList.add(mutualList.size() + 1));
            } catch (InterruptedException e) {
                Assertions.fail(e);
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

//    @Test
    public void testDeadLock() throws InterruptedException {
        EntityLocker<Integer> entityLocker = new ReentrantEntityLocker<>();
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
                Assertions.fail(e);
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
                Assertions.fail(e);
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