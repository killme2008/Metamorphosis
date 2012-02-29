/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.gregor.slave;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.service.Connection;


/**
 * 源自mina的OrderedThreadPoolExecutor，为适应notify remoting做了适当改造
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public class OrderedThreadPoolExecutor extends ThreadPoolExecutor {
    /** A logger for this class (commented as it breaks MDCFlter tests) */
    static Log LOGGER = LogFactory.getLog(OrderedThreadPoolExecutor.class);

    /** A default value for the initial pool size */
    private static final int DEFAULT_INITIAL_THREAD_POOL_SIZE = 0;

    /** A default value for the maximum pool size */
    private static final int DEFAULT_MAX_THREAD_POOL = 16;

    /** A default value for the KeepAlive delay */
    private static final int DEFAULT_KEEP_ALIVE = 30;

    private static final IoCatalog EXIT_SIGNAL = new IoCatalog(new DummyConnection(), null);

    /**
     * A key stored into the session's attribute for the event tasks being
     * queued
     */
    final String TASKS_QUEUE = "tasksQueue" + System.currentTimeMillis();

    /** A queue used to store the available sessions */
    private final BlockingQueue<IoCatalog> waitingIoCatalogs = new LinkedBlockingQueue<IoCatalog>();

    private final Set<Worker> workers = new HashSet<Worker>();

    private volatile int largestPoolSize;
    private final AtomicInteger idleWorkers = new AtomicInteger();

    private long completedTaskCount;
    private volatile boolean shutdown;


    /**
     * Creates a default ThreadPool, with default values : - minimum pool size
     * is 0 - maximum pool size is 16 - keepAlive set to 30 seconds - A default
     * ThreadFactory - All events are accepted
     */
    public OrderedThreadPoolExecutor() {
        this(DEFAULT_INITIAL_THREAD_POOL_SIZE, DEFAULT_MAX_THREAD_POOL, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, Executors
            .defaultThreadFactory());
    }


    /**
     * Creates a default ThreadPool, with default values : - minimum pool size
     * is 0 - keepAlive set to 30 seconds - A default ThreadFactory - All events
     * are accepted
     * 
     * @param maximumPoolSize
     *            The maximum pool size
     */
    public OrderedThreadPoolExecutor(final int maximumPoolSize) {
        this(DEFAULT_INITIAL_THREAD_POOL_SIZE, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, Executors
            .defaultThreadFactory());
    }


    public OrderedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize,
            final ThreadFactory threadFactory) {
        this(corePoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, threadFactory);
    }


    /**
     * Creates a default ThreadPool, with default values : - keepAlive set to 30
     * seconds - A default ThreadFactory - All events are accepted
     * 
     * @param corePoolSize
     *            The initial pool sizePoolSize
     * @param maximumPoolSize
     *            The maximum pool size
     */
    public OrderedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize) {
        this(corePoolSize, maximumPoolSize, DEFAULT_KEEP_ALIVE, TimeUnit.SECONDS, Executors.defaultThreadFactory());
    }


    /**
     * Creates a default ThreadPool, with default values : - A default
     * ThreadFactory - All events are accepted
     * 
     * @param corePoolSize
     *            The initial pool sizePoolSize
     * @param maximumPoolSize
     *            The maximum pool size
     * @param keepAliveTime
     *            Default duration for a thread
     * @param unit
     *            Time unit used for the keepAlive value
     */
    public OrderedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
            final TimeUnit unit) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, Executors.defaultThreadFactory());
    }


    /**
     * Creates a new instance of a OrderedThreadPoolExecutor.
     * 
     * @param corePoolSize
     *            The initial pool sizePoolSize
     * @param maximumPoolSize
     *            The maximum pool size
     * @param keepAliveTime
     *            Default duration for a thread
     * @param unit
     *            Time unit used for the keepAlive value
     * @param threadFactory
     *            The factory used to create threads
     * @param eventQueueHandler
     *            The queue used to store events
     */
    public OrderedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
            final TimeUnit unit, final ThreadFactory threadFactory) {
        // We have to initialize the pool with default values (0 and 1) in order
        // to
        // handle the exception in a better way. We can't add a try {} catch()
        // {}
        // around the super() call.
        super(DEFAULT_INITIAL_THREAD_POOL_SIZE, 1, keepAliveTime, unit, new SynchronousQueue<Runnable>(),
            threadFactory, new AbortPolicy());

        if (corePoolSize < DEFAULT_INITIAL_THREAD_POOL_SIZE) {
            throw new IllegalArgumentException("corePoolSize: " + corePoolSize);
        }

        if (maximumPoolSize == 0 || maximumPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maximumPoolSize: " + maximumPoolSize);
        }

        // Now, we can setup the pool sizes
        super.setCorePoolSize(corePoolSize);
        super.setMaximumPoolSize(maximumPoolSize);

    }


    /**
     * Get the session's tasks queue.
     */
    private TasksQueue getConnectionTasksQueue(final IoCatalog ioCatalog) {
        final Connection connection = ioCatalog.connection;
        final String catalog = this.getCatalog(ioCatalog);
        TasksQueue queue = (TasksQueue) connection.getAttribute(catalog);
        if (queue == null) {
            queue = new TasksQueue();
            final TasksQueue oldQueue = (TasksQueue) connection.setAttributeIfAbsent(catalog, queue);
            if (oldQueue != null) {
                queue = oldQueue;
            }
        }

        return queue;
    }


    private String getCatalog(final IoCatalog ioCatalog) {
        String catalog = ioCatalog.catalog;
        if (StringUtils.isBlank(catalog)) {
            catalog = this.TASKS_QUEUE;
        }
        return catalog;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setRejectedExecutionHandler(final RejectedExecutionHandler handler) {
        // Ignore the request. It must always be AbortPolicy.
    }


    /**
     * Add a new thread to execute a task, if needed and possible. It depends on
     * the current pool size. If it's full, we do nothing.
     */
    private void addWorker() {
        synchronized (this.workers) {
            if (this.workers.size() >= super.getMaximumPoolSize()) {
                return;
            }

            // Create a new worker, and add it to the thread pool
            final Worker worker = new Worker();
            final Thread thread = this.getThreadFactory().newThread(worker);

            // As we have added a new thread, it's considered as idle.
            this.idleWorkers.incrementAndGet();

            // Now, we can start it.
            thread.start();
            this.workers.add(worker);

            if (this.workers.size() > this.largestPoolSize) {
                this.largestPoolSize = this.workers.size();
            }
        }
    }


    /**
     * Add a new Worker only if there are no idle worker.
     */
    private void addWorkerIfNecessary() {
        if (this.idleWorkers.get() == 0) {
            synchronized (this.workers) {
                if (this.workers.isEmpty() || this.idleWorkers.get() == 0) {
                    this.addWorker();
                }
            }
        }
    }


    private void removeWorker() {
        synchronized (this.workers) {
            if (this.workers.size() <= super.getCorePoolSize()) {
                return;
            }
            this.waitingIoCatalogs.offer(EXIT_SIGNAL);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaximumPoolSize() {
        return super.getMaximumPoolSize();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaximumPoolSize(final int maximumPoolSize) {
        if (maximumPoolSize <= 0 || maximumPoolSize < super.getCorePoolSize()) {
            throw new IllegalArgumentException("maximumPoolSize: " + maximumPoolSize);
        }

        synchronized (this.workers) {
            super.setMaximumPoolSize(maximumPoolSize);
            int difference = this.workers.size() - maximumPoolSize;
            while (difference > 0) {
                this.removeWorker();
                --difference;
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {

        final long deadline = System.currentTimeMillis() + unit.toMillis(timeout);

        synchronized (this.workers) {
            while (!this.isTerminated()) {
                final long waitTime = deadline - System.currentTimeMillis();
                if (waitTime <= 0) {
                    break;
                }

                this.workers.wait(waitTime);
            }
        }
        return this.isTerminated();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return this.shutdown;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        if (!this.shutdown) {
            return false;
        }

        synchronized (this.workers) {
            return this.workers.isEmpty();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        if (this.shutdown) {
            return;
        }

        this.shutdown = true;

        synchronized (this.workers) {
            for (int i = this.workers.size(); i > 0; i--) {
                this.waitingIoCatalogs.offer(EXIT_SIGNAL);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<Runnable> shutdownNow() {
        this.shutdown();

        final List<Runnable> answer = new ArrayList<Runnable>();
        IoCatalog ioCatalog;

        while ((ioCatalog = this.waitingIoCatalogs.poll()) != null) {
            if (ioCatalog == EXIT_SIGNAL) {
                this.waitingIoCatalogs.offer(EXIT_SIGNAL);
                Thread.yield(); // Let others take the signal.
                continue;
            }
            final TasksQueue sessionTasksQueue =
                    (TasksQueue) ioCatalog.connection.getAttribute(this.getCatalog(ioCatalog));
            synchronized (sessionTasksQueue.tasksQueue) {
                for (final Runnable task : sessionTasksQueue.tasksQueue) {
                    answer.add(task);
                }
                sessionTasksQueue.tasksQueue.clear();
            }
        }

        return answer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(final Runnable task) {
        if (this.shutdown) {
            this.rejectTask(task);
        }

        // Check that it's a IoEvent task
        this.checkTaskType(task);

        final IoEvent event = (IoEvent) task;

        // Get the associated session
        final IoCatalog ioCatalog = event.getIoCatalog();

        // Get the session's queue of events
        final TasksQueue connectionTasksQueue = this.getConnectionTasksQueue(ioCatalog);
        final Queue<Runnable> tasksQueue = connectionTasksQueue.tasksQueue;

        // Wether to offer new connection
        boolean offerConnection;

        // Ok, the message has been accepted
        synchronized (tasksQueue) {
            // Inject the event into the executor taskQueue
            tasksQueue.offer(event);

            if (connectionTasksQueue.processingCompleted) {
                connectionTasksQueue.processingCompleted = false;
                offerConnection = true;
            }
            else {
                offerConnection = false;
            }
        }

        if (offerConnection) {
            // As the tasksQueue was empty, the task has been executed
            // immediately, so we can move the session to the queue
            // of sessions waiting for completion.
            this.waitingIoCatalogs.offer(ioCatalog);
        }

        this.addWorkerIfNecessary();
    }


    private void rejectTask(final Runnable task) {
        this.getRejectedExecutionHandler().rejectedExecution(task, this);
    }


    private void checkTaskType(final Runnable task) {
        if (!(task instanceof IoEvent)) {
            throw new IllegalArgumentException("task must be an IoEvent or its subclass.");
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveCount() {
        synchronized (this.workers) {
            return this.workers.size() - this.idleWorkers.get();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getCompletedTaskCount() {
        synchronized (this.workers) {
            long answer = this.completedTaskCount;
            for (final Worker w : this.workers) {
                answer += w.completedTaskCount;
            }

            return answer;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getLargestPoolSize() {
        return this.largestPoolSize;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getPoolSize() {
        synchronized (this.workers) {
            return this.workers.size();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getTaskCount() {
        return this.getCompletedTaskCount();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminating() {
        synchronized (this.workers) {
            return this.isShutdown() && !this.isTerminated();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int prestartAllCoreThreads() {
        int answer = 0;
        synchronized (this.workers) {
            for (int i = super.getCorePoolSize() - this.workers.size(); i > 0; i--) {
                this.addWorker();
                answer++;
            }
        }
        return answer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean prestartCoreThread() {
        synchronized (this.workers) {
            if (this.workers.size() < super.getCorePoolSize()) {
                this.addWorker();
                return true;
            }
            else {
                return false;
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public BlockingQueue<Runnable> getQueue() {
        throw new UnsupportedOperationException();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void purge() {
        // Nothing to purge in this implementation.
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Runnable task) {
        this.checkTaskType(task);
        final IoEvent event = (IoEvent) task;
        final IoCatalog ioCatalog = event.getIoCatalog();
        final TasksQueue sessionTasksQueue = (TasksQueue) ioCatalog.connection.getAttribute(this.getCatalog(ioCatalog));
        final Queue<Runnable> tasksQueue = sessionTasksQueue.tasksQueue;
        if (sessionTasksQueue == null) {
            return false;
        }

        boolean removed;

        synchronized (tasksQueue) {
            removed = tasksQueue.remove(task);
        }

        return removed;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getCorePoolSize() {
        return super.getCorePoolSize();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setCorePoolSize(final int corePoolSize) {
        if (corePoolSize < 0) {
            throw new IllegalArgumentException("corePoolSize: " + corePoolSize);
        }
        if (corePoolSize > super.getMaximumPoolSize()) {
            throw new IllegalArgumentException("corePoolSize exceeds maximumPoolSize");
        }

        synchronized (this.workers) {
            if (super.getCorePoolSize() > corePoolSize) {
                for (int i = super.getCorePoolSize() - corePoolSize; i > 0; i--) {
                    this.removeWorker();
                }
            }
            super.setCorePoolSize(corePoolSize);
        }
    }

    private class Worker implements Runnable {

        private volatile long completedTaskCount;
        private Thread thread;


        @Override
        public void run() {
            this.thread = Thread.currentThread();

            try {
                for (;;) {
                    final IoCatalog ioCatalog = this.fetchIoCatalog();

                    OrderedThreadPoolExecutor.this.idleWorkers.decrementAndGet();

                    if (ioCatalog == null) {
                        synchronized (OrderedThreadPoolExecutor.this.workers) {
                            if (OrderedThreadPoolExecutor.this.workers.size() > OrderedThreadPoolExecutor.this
                                .getCorePoolSize()) {
                                // Remove now to prevent duplicate exit.
                                OrderedThreadPoolExecutor.this.workers.remove(this);
                                break;
                            }
                        }
                    }

                    if (ioCatalog == EXIT_SIGNAL) {
                        break;
                    }

                    try {
                        if (ioCatalog != null) {
                            this.runTasks(OrderedThreadPoolExecutor.this.getConnectionTasksQueue(ioCatalog));
                        }
                    }
                    finally {
                        OrderedThreadPoolExecutor.this.idleWorkers.incrementAndGet();
                    }
                }
            }
            finally {
                synchronized (OrderedThreadPoolExecutor.this.workers) {
                    OrderedThreadPoolExecutor.this.workers.remove(this);
                    OrderedThreadPoolExecutor.this.completedTaskCount += this.completedTaskCount;
                    OrderedThreadPoolExecutor.this.workers.notifyAll();
                }
            }
        }


        private IoCatalog fetchIoCatalog() {
            IoCatalog rt = null;
            long currentTime = System.currentTimeMillis();
            final long deadline = currentTime + OrderedThreadPoolExecutor.this.getKeepAliveTime(TimeUnit.MILLISECONDS);
            for (;;) {
                try {
                    final long waitTime = deadline - currentTime;
                    if (waitTime <= 0) {
                        break;
                    }

                    try {
                        rt = OrderedThreadPoolExecutor.this.waitingIoCatalogs.poll(waitTime, TimeUnit.MILLISECONDS);
                        break;
                    }
                    finally {
                        if (rt == null) {
                            currentTime = System.currentTimeMillis();
                        }
                    }
                }
                catch (final InterruptedException e) {
                    // Ignore.
                    continue;
                }
            }
            return rt;
        }


        private void runTasks(final TasksQueue connectionTasksQueue) {
            for (;;) {
                Runnable task;
                final Queue<Runnable> tasksQueue = connectionTasksQueue.tasksQueue;

                synchronized (tasksQueue) {
                    task = tasksQueue.poll();

                    if (task == null) {
                        connectionTasksQueue.processingCompleted = true;
                        break;
                    }
                }

                this.runTask(task);
            }
        }


        private void runTask(final Runnable task) {
            OrderedThreadPoolExecutor.this.beforeExecute(this.thread, task);
            boolean ran = false;
            try {
                task.run();
                ran = true;
                OrderedThreadPoolExecutor.this.afterExecute(task, null);
                this.completedTaskCount++;
            }
            catch (final RuntimeException e) {
                if (!ran) {
                    OrderedThreadPoolExecutor.this.afterExecute(task, e);
                }
                throw e;
            }
        }
    }

    /**
     * A class used to store the ordered list of events to be processed by the
     * session, and the current task state.
     */
    public static class TasksQueue {
        /** A queue of ordered event waiting to be processed */
        private final Queue<Runnable> tasksQueue = new ConcurrentLinkedQueue<Runnable>();

        /** The current task state */
        private boolean processingCompleted = true;
    }
}