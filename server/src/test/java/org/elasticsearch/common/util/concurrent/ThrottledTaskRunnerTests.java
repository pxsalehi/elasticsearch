/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ThrottledTaskRunnerTests extends ESTestCase {
    private ThreadPool threadPool;
    private Executor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test");
        executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    static class TestTask implements Comparable<TestTask>, Runnable {

        private final Runnable runnable;

        TestTask(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public int compareTo(TestTask o) {
            return randomIntBetween(0, 1000);
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    public void testThrottledTaskRunner() throws Exception {
        final int maxTasks = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        ThrottledTaskRunner<TestTask> taskRunner = new ThrottledTaskRunner<>(maxTasks, executor);
        final int enqueued = randomIntBetween(2 * maxTasks, 10 * maxTasks);
        AtomicInteger executed = new AtomicInteger();
        for (int i = 0; i < enqueued; i++) {
            threadPool.generic().execute(() -> {
                taskRunner.enqueueTask(new TestTask(() -> {
                    try {
                        Thread.sleep(randomLongBetween(0, 10));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    executed.incrementAndGet();
                }));
                assertThat(taskRunner.runningTasks(), lessThanOrEqualTo(maxTasks));
            });
        }
        // Eventually all tasks are executed
        assertBusy(() -> {
            assertThat(executed.get(), equalTo(enqueued));
            assertThat(taskRunner.runningTasks(), equalTo(0));
        });
        assertThat(taskRunner.queueSize(), equalTo(0));
    }

    public void testEnqueueSpawnsNewTasksUpToMax() throws Exception {
        int maxTasks = randomIntBetween(1, threadPool.info(ThreadPool.Names.SNAPSHOT).getMax());
        CountDownLatch taskBlocker = new CountDownLatch(1);
        AtomicInteger executed = new AtomicInteger();
        ThrottledTaskRunner<TestTask> taskRunner = new ThrottledTaskRunner<>(maxTasks, executor);
        final int enqueued = maxTasks - 1; // So that it is possible to run at least one more task
        for (int i = 0; i < enqueued; i++) {
            taskRunner.enqueueTask(new TestTask(() -> {
                try {
                    taskBlocker.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed.incrementAndGet();
            }));
            assertThat(taskRunner.runningTasks(), equalTo(i + 1));
        }
        // Enqueueing one or more new tasks would create only one new running task
        final int newTasks = randomIntBetween(1, 10);
        for (int i = 0; i < newTasks; i++) {
            taskRunner.enqueueTask(new TestTask(() -> {
                try {
                    taskBlocker.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executed.incrementAndGet();
            }));
            assertThat(taskRunner.runningTasks(), equalTo(maxTasks));
        }
        taskBlocker.countDown();
        /// Eventually all tasks are executed
        assertBusy(() -> {
            assertThat(executed.get(), equalTo(enqueued + newTasks));
            assertThat(taskRunner.runningTasks(), equalTo(0));
        });
        assertThat(taskRunner.queueSize(), equalTo(0));
    }
}
