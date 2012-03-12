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
 *   dogun (yuexuqiang at gmail.com)
 */
package com.taobao.common.store.journal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import com.taobao.common.store.Store;
import com.taobao.common.store.journal.impl.ConcurrentIndexMap;
import com.taobao.common.store.journal.impl.LRUIndexMap;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.Util;


/**
 * <b>一个通过日志文件实现的key/value对的存储</b>
 * 
 * key必须是16字节 <br />
 * 1、数据文件和日志文件在一起，不记录索引文件<br />
 * name.1 name.1.log<br />
 * 2、data为真正的数据，顺序存放，使用引用计数<br />
 * 3、log为操作+key+偏移量<br />
 * 4、添加数据时，先添加name.1，获得offset和length，然后记录日志，增加引用计数，然后加入或更新内存索引<br />
 * 5、删除数据时，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件<br />
 * 6、获取数据时，直接从内存索引获得数据偏移量<br />
 * 7、更新数据时，调用添加<br />
 * 8、启动时，遍历每一个log文件，通过日志的操作恢复内存索引<br />
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
public class JournalStore implements Store, JournalStoreMBean {
    private final Log log = LogFactory.getLog(JournalStore.class);

    public static final int FILE_SIZE = 1024 * 1024 * 64; // 20M
    // public static final int ONE_DAY = 1000 * 60 * 60 * 24;
    public static final int HALF_DAY = 1000 * 60 * 60 * 12;
    protected static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;
    private final String path;
    private final String name;
    private final boolean force;

    protected IndexMap indices;
    private final Map<BytesKey, Long> lastModifiedMap = new ConcurrentHashMap<BytesKey, Long>();
    public Map<Integer, DataFile> dataFiles = new ConcurrentHashMap<Integer, DataFile>();
    protected Map<Integer, LogFile> logFiles = new ConcurrentHashMap<Integer, LogFile>();

    protected DataFile dataFile = null;
    protected LogFile logFile = null;
    private DataFileAppender dataFileAppender = null;
    private final AtomicInteger number = new AtomicInteger(0);
    private long intervalForCompact = HALF_DAY;
    private long intervalForRemove = HALF_DAY * 2 * 7;
    private volatile ScheduledExecutorService scheduledPool;

    private volatile long maxFileCount = Long.MAX_VALUE;

    protected int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;

    public static class InflyWriteData {
        public volatile byte[] data;
        public volatile int count;


        public InflyWriteData(final byte[] data) {
            super();
            this.data = data;
            this.count = 1;
        }

    }


    /**
     * 默认构造函数，会在path下使用name作为名字生成数据文件
     * 
     * @param path
     * @param name
     * @param force
     * @throws IOException
     */
    public JournalStore(final String path, final String name, final boolean force, final boolean enabledIndexLRU)
            throws IOException {
        this(path, name, null, force, enabledIndexLRU, false);
    }


    /**
     * 自己实现 索引维护组件
     * 
     * @param path
     * @param name
     * @param indices
     * @param force
     * @param enabledIndexLRU
     * @throws IOException
     */
    public JournalStore(final String path, final String name, final IndexMap indices, final boolean force,
            final boolean enabledIndexLRU) throws IOException {
        this(path, name, indices, force, enabledIndexLRU, false);
    }


    /**
     * 
     * @param path
     * @param name
     * @param force
     * @param enableIndexLRU
     * @param enabledDataFileCheck
     * @throws IOException
     */
    public JournalStore(final String path, final String name, final boolean force, final boolean enableIndexLRU,
            final boolean enabledDataFileCheck) throws IOException {
        this(path, name, null, force, enableIndexLRU, false);
    }


    /**
     * 启用数据文件整理的构造函数
     * 
     * @param path
     * @param name
     * @param force
     * @throws IOException
     */
    public JournalStore(final String path, final String name, final IndexMap indices, final boolean force,
            final boolean enableIndexLRU, final boolean enabledDataFileCheck) throws IOException {
        Util.registMBean(this, name);
        this.path = path;
        this.name = name;
        this.force = force;
        if (indices == null) {
            if (enableIndexLRU) {
                final long maxMemory = Runtime.getRuntime().maxMemory();
                // 默认使用最大内存的1/40来存储索引，目前只是估计值，需要调整
                final int capacity = (int) (maxMemory / 40 / 60);
                this.indices =
                        new LRUIndexMap(capacity, this.getPath() + File.separator + name + "_indexCache",
                            enableIndexLRU);
            }
            else {
                this.indices = new ConcurrentIndexMap();
            }
        }
        else {
            this.indices = indices;
        }
        this.dataFileAppender = new DataFileAppender(this);

        this.initLoad();
        // 如果当前没有可用文件，生成
        if (null == this.dataFile || null == this.logFile) {
            this.newDataFile();
        }

        // 启动一个定时线程，对Store4j的数据文件定期进行整理.
        if (enabledDataFileCheck) {
            this.scheduledPool = Executors.newSingleThreadScheduledExecutor();
            this.scheduledPool.scheduleAtFixedRate(new DataFileCheckThread(), this.calcDelay(), HALF_DAY,
                TimeUnit.MILLISECONDS);
            log.warn("启动数据文件定时整理线程");
        }

        // 当应用被关闭的时候,如果没有关闭文件,关闭之.对某些操作系统有用
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    JournalStore.this.close();
                }
                catch (final IOException e) {
                    log.error("close error", e);
                }
            }
        });
    }


    /**
     * 默认构造函数，会在path下使用name作为名字生成数据文件
     * 
     * @param path
     * @param name
     * @throws IOException
     */
    public JournalStore(final String path, final String name) throws IOException {
        this(path, name, false, false);
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#add(byte[], byte[])
     */
    @Override
    public void add(final byte[] key, final byte[] data) throws IOException {
        this.add(key, data, false);
    }


    @Override
    public void add(final byte[] key, final byte[] data, final boolean force) throws IOException {
        // 先检查是否已经存在，如果已经存在抛出异常 判断文件是否满了，添加name.1，获得offset，记录日志，增加引用计数，加入或更新内存索引
        this.checkParam(key, data);
        this.innerAdd(key, data, -1, force);

    }


    @Override
    public boolean remove(final byte[] key, final boolean force) throws IOException {
        return this.innerRemove(key, force);
    }


    /**
     * 用于整体数据文件，能够将数据文件瘦身.
     * 
     * @param key
     * @throws IOException
     */
    private void reuse(final byte[] key, final boolean sync) throws IOException {
        final byte[] value = this.get(key);
        final long oldLastTime = this.lastModifiedMap.get(new BytesKey(key));
        if (value != null && this.remove(key)) {
            this.innerAdd(key, value, oldLastTime, sync);
        }
    }


    /**
     * 计算下个执行周期的delay时间.
     * 
     * @return
     */
    private long calcDelay() {
        final Calendar date = new GregorianCalendar();
        date.setTime(new Date());
        final long currentTime = date.getTime().getTime();

        date.set(Calendar.HOUR_OF_DAY, 6);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);

        long delay = date.getTime().getTime() - currentTime;
        // 超过早上6点，求距离下午6点时间
        if (delay < 0) {
            date.set(Calendar.HOUR_OF_DAY, 18);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            delay = date.getTime().getTime() - currentTime;
            // 超过晚上6点
            if (delay < 0) {
                delay += HALF_DAY;
            }
        }
        return delay;
    }


    /**
     * 内部添加数据
     * 
     * @param key
     * @param data
     * @throws IOException
     */
    private OpItem innerAdd(final byte[] key, final byte[] data, final long oldLastTime, final boolean sync)
            throws IOException {
        final BytesKey k = new BytesKey(key);
        final OpItem op = new OpItem();
        op.op = OpItem.OP_ADD;
        this.dataFileAppender.store(op, k, data, sync);
        this.indices.put(k, op);
        if (oldLastTime == -1) {
            this.lastModifiedMap.put(k, System.currentTimeMillis());
        }
        else {
            this.lastModifiedMap.put(k, oldLastTime);
        }
        return op;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#get(byte[])
     */
    @Override
    public byte[] get(final byte[] key) throws IOException {
        byte[] data = null;
        final BytesKey bytesKey = new BytesKey(key);
        data = this.dataFileAppender.getDataFromInFlyWrites(bytesKey);
        if (data != null) {
            return data;
        }
        final OpItem op = this.indices.get(bytesKey);
        if (null != op) {

            final DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
            if (null != df) {
                final ByteBuffer bf = ByteBuffer.wrap(new byte[op.length]);
                df.read(bf, op.offset);
                data = bf.array();
            }
            else {
                log.warn("数据文件丢失：" + op);
                this.indices.remove(bytesKey);
                this.lastModifiedMap.remove(bytesKey);
            }
        }

        return data;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#iterator()
     */
    @Override
    public Iterator<byte[]> iterator() throws IOException {
        final Iterator<BytesKey> it = this.indices.keyIterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }


            @Override
            public byte[] next() {
                final BytesKey bk = it.next();
                if (null != bk) {
                    return bk.getData();
                }
                return null;
            }


            @Override
            public void remove() {
                throw new UnsupportedOperationException("不支持删除，请直接调用store.remove方法");
            }
        };
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#remove(byte[])
     */
    @Override
    public boolean remove(final byte[] key) throws IOException {
        return this.remove(key, false);
    }


    /**
     * 获得记录在那个文件，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件
     * 
     * @param key
     * @return 是否删除了数据
     * @throws IOException
     */
    private boolean innerRemove(final byte[] key, final boolean sync) throws IOException {
        boolean ret = false;
        final BytesKey k = new BytesKey(key);
        final OpItem op = this.indices.get(k);
        if (null != op) {
            ret = this.innerRemove(op, k, sync);
            if (ret) {
                this.indices.remove(k);
                this.lastModifiedMap.remove(k);
            }
        }
        return ret;
    }


    /**
     * 根据OpItem对象，在日志文件中记录删除的操作日志，并且修改对应数据文件的引用计数.
     * 
     * @param op
     * @return
     * @throws IOException
     */
    private boolean innerRemove(final OpItem op, final BytesKey bytesKey, final boolean sync) throws IOException {
        final DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
        final LogFile lf = this.logFiles.get(Integer.valueOf(op.number));
        if (null != df && null != lf) {
            final OpItem o = new OpItem();
            o.key = op.key;
            o.length = op.length;
            o.number = op.number;
            o.offset = op.offset;
            o.op = OpItem.OP_DEL;
            this.dataFileAppender.remove(o, bytesKey, sync);
            return true;
        }
        return false;
    }


    /**
     * 检查参数是否合法
     * 
     * @param key
     * @param data
     */
    private void checkParam(final byte[] key, final byte[] data) {
        if (null == key || null == data) {
            throw new NullPointerException("key/data can't be null");
        }
        if (key.length != 16) {
            throw new IllegalArgumentException("key.length must be 16");
        }
    }


    /**
     * 生成一个新的数据文件
     * 
     * @throws FileNotFoundException
     */
    protected DataFile newDataFile() throws IOException {
        if (this.dataFiles.size() > this.maxFileCount) {
            throw new RuntimeException("最多只能存储" + this.maxFileCount + "个数据文件");
        }
        final int n = this.number.incrementAndGet();
        this.dataFile = new DataFile(new File(this.path + File.separator + this.name + "." + n), n, this.force);
        this.logFile = new LogFile(new File(this.path + File.separator + this.name + "." + n + ".log"), n, this.force);
        this.dataFiles.put(Integer.valueOf(n), this.dataFile);
        this.logFiles.put(Integer.valueOf(n), this.logFile);
        log.info("生成新文件：" + this.dataFile);
        return this.dataFile;
    }


    /**
     * Create the parent directory if it doesn't exist.
     */
    private void checkParentDir(final File parent) {
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Can't make dir " + this.path);
        }
    }


    /**
     * 类初始化的时候，需要遍历所有的日志文件，恢复内存的索引
     * 
     * @throws IOException
     */
    private void initLoad() throws IOException {
        log.warn("开始恢复数据");
        final String nm = this.name + ".";
        final File dir = new File(this.path);
        this.checkParentDir(dir);
        final File[] fs = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String n) {
                return n.startsWith(nm) && !n.endsWith(".log");
            }
        });
        if (fs == null || fs.length == 0) {
            return;
        }
        log.warn("遍历每个数据文件");
        final List<Integer> indexList = new LinkedList<Integer>();
        for (final File f : fs) {
            try {
                final String fn = f.getName();
                final int n = Integer.parseInt(fn.substring(nm.length()));
                indexList.add(Integer.valueOf(n));
            }
            catch (final Exception e) {
                log.error("parse file index error" + f, e);
            }
        }

        Integer[] indices = indexList.toArray(new Integer[indexList.size()]);

        // 对文件顺序进行排序
        Arrays.sort(indices);

        for (final Integer n : indices) {
            log.warn("处理index为" + n + "的文件");
            // 保存本数据文件的索引信息
            final Map<BytesKey, OpItem> idx = new HashMap<BytesKey, OpItem>();
            // 生成dataFile和logFile
            final File f = new File(dir, this.name + "." + n);
            final DataFile df = new DataFile(f, n, this.force);
            final LogFile lf = new LogFile(new File(f.getAbsolutePath() + ".log"), n, this.force);
            final long size = lf.getLength() / OpItem.LENGTH;

            for (int i = 0; i < size; ++i) { // 循环每一个操作
                final ByteBuffer bf = ByteBuffer.wrap(new byte[OpItem.LENGTH]);
                lf.read(bf, i * OpItem.LENGTH);
                if (bf.hasRemaining()) {
                    log.warn("log file error:" + lf + ", index:" + i);
                    continue;
                }
                final OpItem op = new OpItem();
                op.parse(bf.array());
                final BytesKey key = new BytesKey(op.key);
                switch (op.op) {
                case OpItem.OP_ADD: // 如果是添加的操作，加入索引，增加引用计数
                    final OpItem o = this.indices.get(key);
                    if (null != o) {
                        // 已经在之前添加过，那么必然是Update的时候，Remove的操作日志没有写入。

                        // 写入Remove日志
                        this.innerRemove(o, key, true);

                        // 从map中删除
                        this.indices.remove(key);
                        this.lastModifiedMap.remove(key);
                    }
                    boolean addRefCount = true;
                    if (idx.get(key) != null) {
                        // 在同一个文件中add或者update过，那么只是更新内容，而不增加引用计数。
                        addRefCount = false;
                    }

                    idx.put(key, op);

                    if (addRefCount) {
                        df.increment();
                    }
                    break;

                case OpItem.OP_DEL: // 如果是删除的操作，索引去除，减少引用计数
                    idx.remove(key);
                    df.decrement();
                    break;

                default:
                    log.warn("unknow op:" + (int) op.op);
                    break;
                }
            }
            if (df.getLength() >= FILE_SIZE && df.isUnUsed()) { // 如果这个数据文件已经达到指定大小，并且不再使用，删除
                df.delete();
                lf.delete();
                log.warn("不用了，也超过了大小，删除");
            }
            else { // 否则加入map
                this.dataFiles.put(n, df);
                this.logFiles.put(n, lf);
                if (!df.isUnUsed()) { // 如果有索引，加入总索引
                    this.indices.putAll(idx);
                    // 从新启动后，用日志文件的最后修改时间,这里没有必要非常精确.
                    final long lastModified = lf.lastModified();
                    for (final BytesKey key : idx.keySet()) {
                        this.lastModifiedMap.put(key, lastModified);
                    }
                    log.warn("还在使用，放入索引，referenceCount:" + df.getReferenceCount() + ", index:" + idx.size());
                }
            }
        }
        // 校验加载的文件，并设置当前文件
        if (this.dataFiles.size() > 0) {
            indices = this.dataFiles.keySet().toArray(new Integer[this.dataFiles.keySet().size()]);
            Arrays.sort(indices);
            for (int i = 0; i < indices.length - 1; i++) {
                final DataFile df = this.dataFiles.get(indices[i]);
                if (df.isUnUsed() || df.getLength() < FILE_SIZE) {
                    throw new IllegalStateException("非当前文件的状态是大于等于文件块长度，并且是used状态");
                }
            }
            final Integer n = indices[indices.length - 1];
            this.number.set(n.intValue());
            this.dataFile = this.dataFiles.get(n);
            this.logFile = this.logFiles.get(n);
        }
        log.warn("恢复数据：" + this.size());
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#size()
     */
    @Override
    public int size() {
        return this.indices.size();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#update(byte[], byte[])
     */
    @Override
    public boolean update(final byte[] key, final byte[] data) throws IOException {
        // 对于Update的消息，我们写入OpCode为Update的日志。
        final BytesKey k = new BytesKey(key);
        final OpItem op = this.indices.get(k);
        if (null != op) {
            this.indices.remove(k);
            final OpItem o = this.innerAdd(key, data, -1, false);
            if (o.number != op.number) {
                // 不在同一个文件上更新，才进行删除。
                this.innerRemove(op, k, false);
            }
            else {
                // 同一个文件上更新，减少DataFile引用，因为add的时候会添加
                final DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
                df.decrement();
            }
            return true;
        }
        return false;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFilesInfo()
     */
    @Override
    public String getDataFilesInfo() {
        return this.dataFiles.toString();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFilesInfo()
     */
    @Override
    public String getLogFilesInfo() {
        return this.logFiles.toString();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getNumber()
     */
    @Override
    public int getNumber() {
        return this.number.get();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getPath()
     */
    @Override
    public String getPath() {
        return this.path;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getName()
     */
    @Override
    public String getName() {
        return this.name;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFileInfo()
     */
    @Override
    public String getDataFileInfo() {
        return this.dataFile.toString();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFileInfo()
     */
    @Override
    public String getLogFileInfo() {
        return this.logFile.toString();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#viewIndexMap()
     */
    @Override
    public String viewIndexMap() {
        return this.indices.toString();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.Store#close()
     */
    @Override
    public void close() throws IOException {
        this.sync();
        for (final DataFile df : this.dataFiles.values()) {
            try {
                df.close();
            }
            catch (final Exception e) {
                log.warn("close error:" + df, e);
            }
        }
        this.dataFiles.clear();
        for (final LogFile lf : this.logFiles.values()) {
            try {
                lf.close();
            }
            catch (final Exception e) {
                log.warn("close error:" + lf, e);
            }
        }
        this.logFiles.clear();
        this.indices.close();
        this.lastModifiedMap.clear();
        this.dataFile = null;
        this.logFile = null;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.common.store.journal.JournalStoreMBean#getSize()
     */
    @Override
    public long getSize() throws IOException {
        return this.size();
    }


    @Override
    public long getIntervalForCompact() {
        return this.intervalForCompact;
    }


    @Override
    public void setIntervalForCompact(final long intervalForCompact) {
        this.intervalForCompact = intervalForCompact;
    }


    @Override
    public long getIntervalForRemove() {
        return this.intervalForRemove;
    }


    @Override
    public void setIntervalForRemove(final long intervalForRemove) {
        this.intervalForRemove = intervalForRemove;
    }


    @Override
    public long getMaxFileCount() {
        return this.maxFileCount;
    }


    @Override
    public void setMaxFileCount(final long maxFileCount) {
        this.maxFileCount = maxFileCount;
    }


    public void sync() {
        this.dataFileAppender.sync();
    }


    /**
     * 对数据文件进行检查，并作出相应的处理：
     * 
     * 1.数据超过指定的Remove时间,将会直接删除 2.数据超过指定的Compact时间，先Remove再Add
     * 
     * @throws IOException
     */
    @Override
    public void check() throws IOException {
        final Iterator<byte[]> keys = this.iterator();
        BytesKey key = null;
        final long now = System.currentTimeMillis();
        long time;
        log.warn("Store4j数据文件整理开始...");
        while (keys.hasNext()) {
            key = new BytesKey(keys.next());
            time = this.lastModifiedMap.get(key);
            if (this.intervalForRemove != -1 && now - time > this.intervalForRemove) {
                this.innerRemove(key.getData(), true);
            }
            else if (now - time > this.intervalForCompact) {
                this.reuse(key.getData(), true);
            }
        }
        log.warn("Store4j数据文件整理完毕...");
    }

    /**
     * 数据文件检查的后台线程，主要目的是为了Store4j数据文件瘦身，做的工作如下：
     */
    class DataFileCheckThread implements Runnable {

        @Override
        public void run() {
            try {
                JournalStore.this.check();
            }
            catch (final Exception ex) {
                log.warn("check error:", ex);
            }
        }
    }
}