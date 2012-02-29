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
package com.taobao.metamorphosis.server.store;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.DirectSchedulerFactory;

import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.exception.IllegalTopicException;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.exception.ServiceStartupException;
import com.taobao.metamorphosis.server.exception.WrongPartitionException;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;


/**
 * 消息存储管理器
 * 
 * @author boyan
 * @Date 2011-4-21
 * @author wuhua
 * @Date 2011-6-26
 */
public class MessageStoreManager implements Service {
    // private final class DeletePolicyRunner implements Runnable {
    // @Override
    // public void run() {
    // final long start = System.currentTimeMillis();
    // log.info("Start running delete policy " +
    // MessageStoreManager.this.deletePolicy);
    // for (final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap
    // : MessageStoreManager.this.stores
    // .values()) {
    // if (subMap != null) {
    // for (final MessageStore msgStore : subMap.values()) {
    // if (msgStore != null) {
    // try {
    // msgStore.runDeletePolicy();
    // }
    // catch (final Throwable e) {
    // log.error(
    // "Try to run delete policy with " + msgStore.getTopic() + ","
    // + msgStore.getPartition() + " failed", e);
    // }
    // }
    // }
    // }
    // }
    // log.info("End running delete policy " +
    // MessageStoreManager.this.deletePolicy + " in "
    // + (System.currentTimeMillis() - start) / 1000 + " secs");
    // }
    // }

    private final class FlushRunner implements Runnable {
        int unflushInterval;


        FlushRunner(final int unflushInterval) {
            this.unflushInterval = unflushInterval;
        }


        @Override
        public void run() {
            for (final ConcurrentHashMap<Integer, MessageStore> map : MessageStoreManager.this.stores.values()) {
                for (final MessageStore store : map.values()) {
                    if (this.unflushInterval != MessageStoreManager.this.metaConfig.getTopicConfig(store.getTopic())
                        .getUnflushInterval()) {
                        continue;
                    }
                    try {
                        store.flush();
                    }
                    catch (final IOException e) {
                        log.error("Try to flush store failed", e);
                    }
                }
            }
        }
    }

    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> stores =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, MessageStore>>();
    private final MetaConfig metaConfig;
    private ScheduledThreadPoolExecutor scheduledExecutorService;// =
                                                                 // Executors.newScheduledThreadPool(2);
    static final Log log = LogFactory.getLog(MessageStoreManager.class);

    private final DeletePolicy deletePolicy;

    private DeletePolicySelector deletePolicySelector;

    public static final int HALF_DAY = 1000 * 60 * 60 * 12;

    private final Set<Pattern> topicsPatSet = new HashSet<Pattern>();

    private final ConcurrentHashMap<Integer, ScheduledFuture<?>> unflushIntervalMap =
            new ConcurrentHashMap<Integer, ScheduledFuture<?>>();

    private Scheduler scheduler;


    public MessageStoreManager(final MetaConfig metaConfig, final DeletePolicy deletePolicy) {
        super();
        this.metaConfig = metaConfig;
        this.deletePolicy = deletePolicy;
        this.newDeletePolicySelector();
        this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                MessageStoreManager.this.makeTopicsPatSet();
                MessageStoreManager.this.newDeletePolicySelector();
            }
        });

        this.metaConfig.addPropertyChangeListener("unflushInterval", new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                MessageStoreManager.this.scheduleFlushTask();
            }
        });

        this.makeTopicsPatSet();

        this.initScheduler();

        // 定时flush
        this.scheduleFlushTask();

        // this.scheduledExecutorService.scheduleAtFixedRate(new FlushRunner(),
        // metaConfig.getUnflushInterval(),
        // metaConfig.getUnflushInterval(), TimeUnit.MILLISECONDS);

        // 定时做删除
        // this.scheduledExecutorService.scheduleAtFixedRate(new
        // DeletePolicyRunner(), this.calcDelay(), HALF_DAY,
        // TimeUnit.MILLISECONDS);

    }


    /** 根据flush时间间隔分类，分别提交定时任务 */
    private void scheduleFlushTask() {
        log.info("Begin schedule flush task...");
        final Set<Integer> newUnflushIntervals = new HashSet<Integer>();
        for (final String topic : this.metaConfig.getTopics()) {
            newUnflushIntervals.add(this.metaConfig.getTopicConfig(topic).getUnflushInterval());
        }

        // 调整线程池大小
        if (newUnflushIntervals.size() != this.unflushIntervalMap.size()) {
            this.scheduledExecutorService.setCorePoolSize(newUnflushIntervals.size() + 1);
        }

        // 新的有，旧的没有，提交任务
        for (final Integer unflushInterval : newUnflushIntervals) {
            if (!this.unflushIntervalMap.containsKey(unflushInterval) && unflushInterval > 0) {
                final ScheduledFuture<?> future =
                        this.scheduledExecutorService.scheduleAtFixedRate(new FlushRunner(unflushInterval),
                            unflushInterval, unflushInterval, TimeUnit.MILLISECONDS);
                this.unflushIntervalMap.put(unflushInterval, future);
                log.info("Create flush task,unflushInterval=" + unflushInterval);
            }
        }

        // 新的没有，旧的有，销毁任务
        final Set<Integer> set = new HashSet<Integer>(this.unflushIntervalMap.keySet());
        for (final Integer unflushInterval : set) {
            if (!newUnflushIntervals.contains(unflushInterval)) {
                final ScheduledFuture<?> future = this.unflushIntervalMap.remove(unflushInterval);
                if (future != null) {
                    future.cancel(false);
                    log.info("Cancel flush task,unflushInterval=" + unflushInterval);
                }
            }
        }

        this.scheduledExecutorService.purge();
        log.info("Schedule flush task finished. CorePoolSize=" + this.scheduledExecutorService.getCorePoolSize()
                + ",current pool size=" + this.scheduledExecutorService.getPoolSize());
    }


    /** 初始化定时线程池 */
    private void initScheduler() {
        // 根据定时flush时间间隔分类,计算定时线程池大小,并初始化
        final Set<Integer> tmpSet = new HashSet<Integer>();
        for (final String topic : this.metaConfig.getTopics()) {
            final int unflushInterval = this.metaConfig.getTopicConfig(topic).getUnflushInterval();
            tmpSet.add(unflushInterval);
        }
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(tmpSet.size() + 5);

        try {
            if (DirectSchedulerFactory.getInstance().getAllSchedulers().isEmpty()) {
                DirectSchedulerFactory.getInstance().createVolatileScheduler(this.metaConfig.getQuartzThreadCount());
            }
            this.scheduler = DirectSchedulerFactory.getInstance().getScheduler();
        }
        catch (final SchedulerException e) {
            throw new ServiceStartupException("Initialize quartz scheduler failed", e);
        }
    }


    private void newDeletePolicySelector() {
        this.deletePolicySelector = new DeletePolicySelector(this.metaConfig);
    }


    private void makeTopicsPatSet() {
        for (String topic : this.metaConfig.getTopics()) {
            topic = topic.replaceAll("\\*", ".*");
            this.topicsPatSet.add(Pattern.compile(topic));
        }
    }

    /**
     * @author wuhua
     */
    static class DeletePolicySelector {
        private final Map<String, DeletePolicy> deletePolicyMap = new HashMap<String, DeletePolicy>();


        DeletePolicySelector(final MetaConfig metaConfig) {
            for (final String topic : metaConfig.getTopics()) {
                final TopicConfig topicConfig = metaConfig.getTopicConfig(topic);
                final String deletePolicy =
                        topicConfig != null ? topicConfig.getDeletePolicy() : metaConfig.getDeletePolicy();
                this.deletePolicyMap.put(topic, DeletePolicyFactory.getDeletePolicy(deletePolicy));
            }
        }


        DeletePolicy select(final String topic, final DeletePolicy defaultPolicy) {
            final DeletePolicy deletePolicy = this.deletePolicyMap.get(topic);
            return deletePolicy != null ? deletePolicy : defaultPolicy;
        }
    }


    public Map<String/* topic */, ConcurrentHashMap<Integer/* partition */, MessageStore>> getMessageStores() {
        return Collections.unmodifiableMap(this.stores);
    }


    public long getTotalMessagesCount() {
        long rt = 0;
        for (final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap : MessageStoreManager.this.stores
            .values()) {
            if (subMap != null) {
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        rt += msgStore.getMessageCount();
                    }
                }
            }
        }
        return rt;
    }


    public int getTopicCount() {
        return this.stores.size();
    }


    private Set<File> getDataDirSet(final MetaConfig metaConfig) throws IOException {
        final Set<String> paths = new HashSet<String>();
        // public data path
        paths.add(metaConfig.getDataPath());
        // topic data path
        for (final String topic : metaConfig.getTopics()) {
            final TopicConfig topicConfig = metaConfig.getTopicConfig(topic);
            if (topicConfig != null) {
                paths.add(topicConfig.getDataPath());
            }
        }
        final Set<File> fileSet = new HashSet<File>();
        for (final String path : paths) {
            fileSet.add(this.getDataDir(path));
        }
        return fileSet;
    }


    private void loadMessageStores(final MetaConfig metaConfig) throws IOException {
        for (final File dir : this.getDataDirSet(metaConfig)) {
            this.loadDataDir(metaConfig, dir);
        }
    }


    private void loadDataDir(final MetaConfig metaConfig, final File dir) throws IOException {
        log.warn("Begin to scan data path:" + dir.getAbsolutePath());
        final long start = System.currentTimeMillis();
        final File[] ls = dir.listFiles();
        for (final File subDir : ls) {
            if (!subDir.isDirectory()) {
                log.warn("Ignore not directory path:" + subDir.getAbsolutePath());
            }
            else {
                final String name = subDir.getName();
                final int index = name.lastIndexOf('-');
                if (index < 0) {
                    log.warn("Ignore invlaid directory:" + subDir.getAbsolutePath());
                    continue;
                }
                log.warn("Loading data directory:" + subDir.getAbsolutePath() + "...");
                final String topic = name.substring(0, index);
                final int partition = Integer.parseInt(name.substring(index + 1));

                final MessageStore messageStore =
                        new MessageStore(topic, partition, metaConfig, this.deletePolicySelector.select(topic,
                            this.deletePolicy));
                ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
                if (map == null) {
                    map = new ConcurrentHashMap<Integer, MessageStore>();
                    this.stores.put(topic, map);
                }
                map.put(partition, messageStore);
            }
        }
        log.warn("End to scan data path in " + (System.currentTimeMillis() - start) / 1000 + " secs");
    }


    private File getDataDir(final String path) throws IOException {
        final File dir = new File(path);
        if (!dir.exists() && !dir.mkdir()) {
            throw new IOException("Could not make data directory " + dir.getAbsolutePath());
        }
        if (!dir.isDirectory() || !dir.canRead()) {
            throw new IOException("Data path " + dir.getAbsolutePath() + " is not a readable directory");
        }
        return dir;
    }

    private final Random random = new Random();


    public int chooseRandomPartition(final String topic) {
        return this.random.nextInt(this.getNumPartitions(topic));
    }


    /**
     * 计算下个执行周期的delay时间.每天执行两次：早上6点或者晚上6点
     * 
     * @return
     */
    @Deprecated
    private long calcDelay() {
        final Calendar date = new GregorianCalendar();
        date.setTime(new Date());
        final long currentTime = date.getTime().getTime();

        date.set(Calendar.HOUR_OF_DAY, 6);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);

        long delay = date.getTimeInMillis() - currentTime;
        // 超过早上6点，求距离下午6点时间
        if (delay < 0) {
            date.set(Calendar.HOUR_OF_DAY, 18);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            delay = date.getTimeInMillis() - currentTime;
            // 超过晚上6点，加上12个小时等待明天早上6点
            if (delay < 0) {
                delay += HALF_DAY;
            }
        }
        return delay;
    }


    public int getNumPartitions(final String topic) {
        final TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
        return topicConfig != null ? topicConfig.getNumPartitions() : this.metaConfig.getNumPartitions();
    }


    @Override
    public void dispose() {
        this.scheduledExecutorService.shutdown();
        if (this.scheduler != null) {
            try {
                this.scheduler.shutdown(true);
            }
            catch (final SchedulerException e) {
                log.error("Shutdown quartz scheduler failed", e);
            }
        }
        for (final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap : MessageStoreManager.this.stores
            .values()) {
            if (subMap != null) {
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        try {
                            msgStore.close();
                        }
                        catch (final Throwable e) {
                            log.error("Try to run close  " + msgStore.getTopic() + "," + msgStore.getPartition()
                                    + " failed", e);
                        }
                    }
                }
            }
        }
        this.stores.clear();
    }


    @Override
    public void init() {
        // 加载已有数据并校验
        try {
            this.loadMessageStores(this.metaConfig);
        }
        catch (final IOException e) {
            log.error("load message stores failed", e);
            throw new MetamorphosisServerStartupException("Initilize message store manager failed", e);
        }

        this.startScheduleDeleteJobs();
    }


    private void startScheduleDeleteJobs() {
        final Map<String/* deleteWhen */, JobInfo> jobs = new HashMap<String, MessageStoreManager.JobInfo>();
        // 启动quartz job
        for (final String topic : this.getAllTopics()) {
            final TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
            final String deleteWhen =
                    topicConfig != null ? topicConfig.getDeleteWhen() : this.metaConfig.getDeleteWhen();
            JobInfo jobInfo = jobs.get(deleteWhen);
            if (jobInfo == null) {
                final JobDetail job = newJob(DeleteJob.class).build();
                job.getJobDataMap().put(DeleteJob.TOPICS, new HashSet<String>());
                job.getJobDataMap().put(DeleteJob.STORE_MGR, this);
                final Trigger trigger = newTrigger().withSchedule(cronSchedule(deleteWhen)).forJob(job).build();
                jobInfo = new JobInfo(job, trigger);
                jobs.put(deleteWhen, jobInfo);
            }
            // 添加本topic
            ((Set<String>) jobInfo.job.getJobDataMap().get(DeleteJob.TOPICS)).add(topic);
        }

        for (final JobInfo jobInfo : jobs.values()) {
            try {
                this.scheduler.scheduleJob(jobInfo.job, jobInfo.trigger);
            }
            catch (final SchedulerException e) {
                throw new ServiceStartupException("Schedule delete job failed", e);
            }
        }
        try {
            this.scheduler.start();
        }
        catch (final SchedulerException e) {
            throw new ServiceStartupException("Start scheduler failed", e);
        }
    }

    private static class JobInfo {
        public final JobDetail job;
        public final Trigger trigger;


        public JobInfo(final JobDetail job, final Trigger trigger) {
            super();
            this.job = job;
            this.trigger = trigger;
        }

    }


    private Set<String> getAllTopics() {
        final Set<String> rt = new HashSet<String>();
        rt.addAll(this.metaConfig.getTopics());
        rt.addAll(this.getMessageStores().keySet());
        return rt;
    }


    public MessageStore getMessageStore(final String topic, final int partition) {
        final ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(partition);
    }


    Collection<MessageStore> getMessageStoresByTopic(final String topic) {
        final ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            return Collections.emptyList();
        }
        return map.values();
    }


    public MessageStore getOrCreateMessageStore(final String topic, final int partition) throws IOException {
        return this.getOrCreateMessageStoreInner(topic, partition, 0);
    }


    public MessageStore getOrCreateMessageStore(final String topic, final int partition, final long offsetIfCreate)
            throws IOException {
        return this.getOrCreateMessageStoreInner(topic, partition, offsetIfCreate);
    }


    private MessageStore getOrCreateMessageStoreInner(final String topic, final int partition, final long offsetIfCreate)
            throws IOException {
        if (!this.isLegalTopic(topic)) {
            throw new IllegalTopicException("The server do not accept topic " + topic);
        }
        if (partition < 0 || partition >= this.getNumPartitions(topic)) {
            log.warn("Wrong partition " + partition + ",valid partitions (0," + (this.getNumPartitions(topic) - 1)
                    + ")");
            throw new WrongPartitionException("wrong partition " + partition);
        }
        ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            map = new ConcurrentHashMap<Integer, MessageStore>();
            final ConcurrentHashMap<Integer/* partition */, MessageStore> oldMap = this.stores.putIfAbsent(topic, map);
            if (oldMap != null) {
                map = oldMap;
            }
        }

        MessageStore messageStore = map.get(partition);
        if (messageStore != null) {
            return messageStore;
        }
        else {
            // 对string加锁，特例
            synchronized (topic.intern()) {
                messageStore = map.get(partition);
                // double check
                if (messageStore != null) {
                    return messageStore;
                }
                messageStore =
                        new MessageStore(topic, partition, this.metaConfig, this.deletePolicySelector.select(topic,
                            this.deletePolicy), offsetIfCreate);
                log.info("新创建消息存储，topic=" + topic + ",partition=" + partition);
                map.put(partition, messageStore);

            }
        }
        return messageStore;
    }


    boolean isLegalTopic(final String topic) {
        for (final Pattern pat : this.topicsPatSet) {
            if (pat.matcher(topic).matches()) {
                return true;
            }
        }
        return false;

    }
}