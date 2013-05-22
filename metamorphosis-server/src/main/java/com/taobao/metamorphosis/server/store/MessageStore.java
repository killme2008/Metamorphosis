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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.SystemTimer;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 一个topic的消息存储，内部管理多个文件(segment)
 * 
 * @author boyan
 * @Date 2011-4-20
 * @author wuhua
 * @Date 2011-6-26
 * 
 */
public class MessageStore extends Thread implements Closeable {
    private static final int ONE_M_BYTES = 512 * 1024;
    private static final String FILE_SUFFIX = ".meta";
    private volatile boolean closed = false;
    static final Log log = LogFactory.getLog(MessageStore.class);

    // 表示一个消息文件
    static class Segment {
        // 该片段代表的offset
        final long start;
        // 对应的文件
        final File file;
        // 该片段的消息集合
        FileMessageSet fileMessageSet;


        public Segment(final long start, final File file) {
            this(start, file, true);
        }


        public Segment(final long start, final File file, final boolean mutable) {
            super();
            this.start = start;
            this.file = file;
            log.info("Created segment " + this.file.getAbsolutePath());
            try {
                final FileChannel channel = new RandomAccessFile(this.file, "rw").getChannel();
                this.fileMessageSet = new FileMessageSet(channel, 0, channel.size(), mutable);
                // // 不可变的，这里不能直接用FileMessageSet(channel, false)
                // if (mutable == true) {
                // this.fileMessageSet.setMutable(true);
                // }
            }
            catch (final IOException e) {
                log.error("初始化消息集合失败", e);
            }
        }


        public long size() {
            return this.fileMessageSet.highWaterMark();
        }


        // 判断offset是否在本文件内
        public boolean contains(final long offset) {
            if (this.size() == 0 && offset == this.start || this.size() > 0 && offset >= this.start
                    && offset <= this.start + this.size() - 1) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    /**
     * 不可变的segment list
     * 
     * @author boyan
     * @Date 2011-4-20
     * 
     */
    static class SegmentList {
        AtomicReference<Segment[]> contents = new AtomicReference<Segment[]>();


        public SegmentList(final Segment[] s) {
            this.contents.set(s);
        }


        public SegmentList() {
            super();
            this.contents.set(new Segment[0]);
        }


        public void append(final Segment segment) {
            while (true) {
                final Segment[] curr = this.contents.get();
                final Segment[] update = new Segment[curr.length + 1];
                System.arraycopy(curr, 0, update, 0, curr.length);
                update[curr.length] = segment;
                if (this.contents.compareAndSet(curr, update)) {
                    return;
                }
            }
        }


        public void delete(final Segment segment) {
            while (true) {
                final Segment[] curr = this.contents.get();
                int index = -1;
                for (int i = 0; i < curr.length; i++) {
                    if (curr[i] == segment) {
                        index = i;
                        break;
                    }

                }
                if (index == -1) {
                    return;
                }
                final Segment[] update = new Segment[curr.length - 1];
                // 拷贝前半段
                System.arraycopy(curr, 0, update, 0, index);
                // 拷贝后半段
                if (index + 1 < curr.length) {
                    System.arraycopy(curr, index + 1, update, index, curr.length - index - 1);
                }
                if (this.contents.compareAndSet(curr, update)) {
                    return;
                }
            }
        }


        public Segment[] view() {
            return this.contents.get();
        }


        public Segment last() {
            final Segment[] copy = this.view();
            if (copy.length > 0) {
                return copy[copy.length - 1];
            }
            return null;
        }


        public Segment first() {
            final Segment[] copy = this.view();
            if (copy.length > 0) {
                return copy[0];
            }
            return null;
        }

    }

    private SegmentList segments;
    private final File partitionDir;
    private final String topic;
    private final int partition;
    private final AtomicInteger unflushed;
    private final AtomicLong lastFlushTime;
    private final MetaConfig metaConfig;
    private final DeletePolicy deletePolicy;
    private final LinkedTransferQueue<WriteRequest> bufferQueue = new LinkedTransferQueue<WriteRequest>();
    private long maxTransferSize;
    int unflushThreshold = 1000;


    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig,
            final DeletePolicy deletePolicy) throws IOException {
        this(topic, partition, metaConfig, deletePolicy, 0);
    }

    private volatile String desc;


    public String getDescription() {
        if (this.desc == null) {
            this.desc = this.topic + "-" + this.partition;
        }
        return this.desc;
    }


    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig,
            final DeletePolicy deletePolicy, final long offsetIfCreate) throws IOException {
        this.metaConfig = metaConfig;
        this.topic = topic;
        final TopicConfig topicConfig = this.metaConfig.getTopicConfig(this.topic);
        String dataPath = metaConfig.getDataPath();
        if (topicConfig != null) {
            dataPath = topicConfig.getDataPath();
        }
        final File parentDir = new File(dataPath);
        this.checkDir(parentDir);
        this.partitionDir = new File(dataPath + File.separator + topic + "-" + partition);
        this.checkDir(this.partitionDir);
        // this.topic = topic;
        this.partition = partition;
        this.unflushed = new AtomicInteger(0);
        this.lastFlushTime = new AtomicLong(SystemTimer.currentTimeMillis());
        this.unflushThreshold = topicConfig.getUnflushThreshold();
        this.deletePolicy = deletePolicy;

        // Make a copy to avoid getting it again and again.
        this.maxTransferSize = metaConfig.getMaxTransferSize();
        this.maxTransferSize = this.maxTransferSize > ONE_M_BYTES ? ONE_M_BYTES : this.maxTransferSize;

        // Check directory and load exists segments.
        this.checkDir(this.partitionDir);
        this.loadSegments(offsetIfCreate);
        if (this.useGroupCommit()) {
            this.start();
        }
    }


    public long getMessageCount() {
        long sum = 0;
        for (final Segment seg : this.segments.view()) {
            sum += seg.fileMessageSet.getMessageCount();
        }
        return sum;
    }


    public long getSizeInBytes() {
        long sum = 0;
        for (final Segment seg : this.segments.view()) {
            sum += seg.fileMessageSet.getSizeInBytes();
        }
        return sum;
    }


    SegmentList getSegments() {
        return this.segments;
    }


    File getPartitionDir() {
        return this.partitionDir;
    }


    @Override
    public void close() throws IOException {
        this.closed = true;
        this.interrupt();
        try {
            this.join(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (final Segment segment : this.segments.view()) {
            segment.fileMessageSet.close();
        }
    }


    public void runDeletePolicy() {
        if (this.deletePolicy == null) {
            return;
        }
        final long start = System.currentTimeMillis();
        final Segment[] view = this.segments.view();
        for (final Segment segment : view) {
            // 非可变并且可删除
            if (!segment.fileMessageSet.isMutable() && this.deletePolicy.canDelete(segment.file, start)) {
                log.info("Deleting file " + segment.file.getAbsolutePath() + " with policy " + this.deletePolicy.name());
                this.segments.delete(segment);
                try {
                    segment.fileMessageSet.close();
                    this.deletePolicy.process(segment.file);
                }
                catch (final IOException e) {
                    log.error("关闭并删除file message set失败", e);
                }

            }
        }

    }


    /**
     * 加载并校验文件
     */
    private void loadSegments(final long offsetIfCreate) throws IOException {
        final List<Segment> accum = new ArrayList<Segment>();
        final File[] ls = this.partitionDir.listFiles();

        if (ls != null) {
            for (final File file : ls) {
                if (file.isFile() && file.toString().endsWith(FILE_SUFFIX)) {
                    if (!file.canRead()) {
                        throw new IOException("Could not read file " + file);
                    }
                    final String filename = file.getName();
                    final long start = Long.parseLong(filename.substring(0, filename.length() - FILE_SUFFIX.length()));
                    // 先作为不可变的加载进来
                    accum.add(new Segment(start, file, false));
                }
            }
        }

        if (accum.size() == 0) {
            // 没有可用的文件，创建一个，索引从offsetIfCreate开始
            final File newFile = new File(this.partitionDir, this.nameFromOffset(offsetIfCreate));
            accum.add(new Segment(offsetIfCreate, newFile));
        }
        else {
            // 至少有一个文件，校验并按照start升序排序
            Collections.sort(accum, new Comparator<Segment>() {
                @Override
                public int compare(final Segment o1, final Segment o2) {
                    if (o1.start == o2.start) {
                        return 0;
                    }
                    else if (o1.start > o2.start) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                }
            });
            // 校验文件
            this.validateSegments(accum);
            // 最后一个文件修改为可变
            final Segment last = accum.remove(accum.size() - 1);
            last.fileMessageSet.close();
            log.info("Loading the last segment in mutable mode and running recover on " + last.file.getAbsolutePath());
            final Segment mutable = new Segment(last.start, last.file);
            accum.add(mutable);
            log.info("Loaded " + accum.size() + " segments...");
        }

        this.segments = new SegmentList(accum.toArray(new Segment[accum.size()]));
    }


    private void validateSegments(final List<Segment> segments) {
        this.writeLock.lock();
        try {
            for (int i = 0; i < segments.size() - 1; i++) {
                final Segment curr = segments.get(i);
                final Segment next = segments.get(i + 1);
                if (curr.start + curr.size() != next.start) {
                    throw new IllegalStateException("The following segments don't validate: "
                            + curr.file.getAbsolutePath() + ", " + next.file.getAbsolutePath());
                }
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }


    private void checkDir(final File dir) {
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new RuntimeException("Create directory failed:" + dir.getAbsolutePath());
            }
        }
        if (!dir.isDirectory()) {
            throw new RuntimeException("Path is not a directory:" + dir.getAbsolutePath());
        }
    }

    private final ReentrantLock writeLock = new ReentrantLock();


    /**
     * Append单个消息，返回写入的位置
     * 
     * @param msgId
     * @param req
     * @return
     */
    public void append(final long msgId, final PutCommand req, final AppendCallback cb) {
        this.appendBuffer(MessageUtils.makeMessageBuffer(msgId, req), cb);
    }

    private static class WriteRequest {
        public final ByteBuffer buf;
        public final AppendCallback cb;
        public Location result;


        public WriteRequest(final ByteBuffer buf, final AppendCallback cb) {
            super();
            this.buf = buf;
            this.cb = cb;
        }
    }


    private void appendBuffer(final ByteBuffer buffer, final AppendCallback cb) {
        if (this.closed) {
            throw new IllegalStateException("Closed MessageStore.");
        }
        if (this.useGroupCommit() && buffer.remaining() < this.maxTransferSize) {
            this.bufferQueue.offer(new WriteRequest(buffer, cb));
        }
        else {
            Location location = null;
            final int remainning = buffer.remaining();
            this.writeLock.lock();
            try {
                final Segment cur = this.segments.last();
                final long offset = cur.start + cur.fileMessageSet.append(buffer);
                this.mayBeFlush(1);
                this.mayBeRoll();
                location = Location.create(offset, remainning);
            }
            catch (final IOException e) {
                log.error("Append file failed", e);
                location = Location.InvalidLocaltion;
            }
            finally {
                this.writeLock.unlock();
                if (cb != null) {
                    cb.appendComplete(location);
                }
            }
        }
    }


    private void notifyCallback(AppendCallback callback, Location location) {
        try {
            callback.appendComplete(location);
        }
        catch (Exception e) {
            log.error("Call AppendCallback failed", e);
        }
    }


    private boolean useGroupCommit() {
        return this.unflushThreshold <= 0;
    }


    @Override
    public void run() {
        // 等待force的队列
        final LinkedList<WriteRequest> toFlush = new LinkedList<WriteRequest>();
        WriteRequest req = null;
        long lastFlushPos = 0;
        Segment last = null;
        while (!this.closed && !Thread.currentThread().isInterrupted()) {
            try {
                if (last == null) {
                    last = this.segments.last();
                    lastFlushPos = last.fileMessageSet.highWaterMark();
                }
                if (req == null) {
                    if (toFlush.isEmpty()) {
                        req = this.bufferQueue.take();
                    }
                    else {
                        req = this.bufferQueue.poll();
                        if (req == null || last.fileMessageSet.getSizeInBytes() > lastFlushPos + this.maxTransferSize) {
                            // 强制force
                            last.fileMessageSet.flush();
                            lastFlushPos = last.fileMessageSet.highWaterMark();
                            // 通知回调
                            for (final WriteRequest request : toFlush) {
                                request.cb.appendComplete(request.result);
                            }
                            toFlush.clear();
                            // 是否需要roll
                            this.mayBeRoll();
                            // 如果切换文件，重新获取last
                            if (this.segments.last() != last) {
                                last = null;
                            }
                            continue;
                        }
                    }
                }

                if (req == null) {
                    continue;
                }
                final int remainning = req.buf.remaining();
                final long offset = last.start + last.fileMessageSet.append(req.buf);
                req.result = Location.create(offset, remainning);
                if (req.cb != null) {
                    toFlush.add(req);
                }
                req = null;
            }
            catch (final IOException e) {
                log.error("Append message failed,*critical error*,the group commit thread would be terminated.", e);
                // TODO io异常没办法处理了，简单跳出?
                break;
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }
        // terminated
        try {
            for (WriteRequest request : this.bufferQueue) {
                final int remainning = request.buf.remaining();
                final long offset = last.start + last.fileMessageSet.append(request.buf);
                if (request.cb != null) {
                    request.cb.appendComplete(Location.create(offset, remainning));
                }
            }
            this.bufferQueue.clear();
        }
        catch (IOException e) {
            log.error("Append message failed", e);
        }

    }


    /**
     * Append多个消息，返回写入的位置
     * 
     * @param msgIds
     * @param reqs
     * 
     * @return
     */
    public void append(final List<Long> msgIds, final List<PutCommand> putCmds, final AppendCallback cb) {
        this.appendBuffer(MessageUtils.makeMessageBuffer(msgIds, putCmds), cb);
    }


    /**
     * 重放事务操作，如果消息没有存储成功，则重新存储，并返回新的位置
     * 
     * @param to
     * @param msgIds
     * @param reqs
     * @return
     * @throws IOException
     */
    public void replayAppend(final long offset, final int length, final int checksum, final List<Long> msgIds,
            final List<PutCommand> reqs, final AppendCallback cb) throws IOException {
        final Segment segment = this.findSegment(this.segments.view(), offset);
        if (segment == null) {
            this.append(msgIds, reqs, cb);
        }
        else {
            final MessageSet messageSet =
                    segment.fileMessageSet.slice(offset - segment.start, offset - segment.start + length);
            final ByteBuffer buf = ByteBuffer.allocate(length);
            messageSet.read(buf, offset - segment.start);
            buf.flip();
            final byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            // 这个校验和是整个消息的校验和，这跟message的校验和不一样，注意区分
            final int checkSumInDisk = CheckSum.crc32(bytes);
            // 没有存入，则重新存储
            if (checksum != checkSumInDisk) {
                this.append(msgIds, reqs, cb);
            }
            else {
                // 正常存储了消息，无需处理
                if (cb != null) {
                    this.notifyCallback(cb, null);
                }
            }
        }
    }


    public String getTopic() {
        return this.topic;
    }


    public int getPartition() {
        return this.partition;
    }


    private void mayBeRoll() throws IOException {
        if (this.segments.last().fileMessageSet.getSizeInBytes() >= this.metaConfig.getMaxSegmentSize()) {
            this.roll();
        }
    }


    String nameFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + FILE_SUFFIX;
    }


    private void roll() throws IOException {
        final long newOffset = this.nextAppendOffset();
        final File newFile = new File(this.partitionDir, this.nameFromOffset(newOffset));
        this.segments.last().fileMessageSet.flush();
        this.segments.last().fileMessageSet.setMutable(false);
        this.segments.append(new Segment(newOffset, newFile));
    }


    private long nextAppendOffset() throws IOException {
        final Segment last = this.segments.last();
        last.fileMessageSet.flush();
        return last.start + last.size();
    }


    private void mayBeFlush(final int numOfMessages) throws IOException {
        if (this.unflushed.addAndGet(numOfMessages) > this.metaConfig.getTopicConfig(this.topic).getUnflushThreshold()
                || SystemTimer.currentTimeMillis() - this.lastFlushTime.get() > this.metaConfig.getTopicConfig(
                    this.topic).getUnflushInterval()) {
            this.flush0();
        }
    }


    public List<SegmentInfo> getSegmentInfos() {
        final List<SegmentInfo> rt = new ArrayList<SegmentInfo>();
        for (final Segment seg : this.segments.view()) {
            rt.add(new SegmentInfo(seg.start, seg.size()));
        }
        return rt;
    }


    public void flush() throws IOException {
        this.writeLock.lock();
        try {
            this.flush0();
        }
        finally {
            this.writeLock.unlock();
        }
    }


    private void flush0() throws IOException {
        if (this.useGroupCommit()) {
            return;
        }
        this.segments.last().fileMessageSet.flush();
        this.unflushed.set(0);
        this.lastFlushTime.set(SystemTimer.currentTimeMillis());
    }


    /**
     * 返回当前最大可读的offset
     * 
     * @return
     */
    public long getMaxOffset() {
        final Segment last = this.segments.last();
        if (last != null) {
            return last.start + last.size();
        }
        else {
            return 0;
        }
    }


    /**
     * 返回当前最小可读的offset
     * 
     * @return
     */
    public long getMinOffset() {
        Segment first = this.segments.first();
        if (first != null) {
            return first.start;
        }
        else {
            return 0;
        }
    }


    /**
     * 根据offset和maxSize返回所在MessageSet, 当offset超过最大offset的时候返回null，
     * 当offset小于最小offset的时候抛出ArrayIndexOutOfBounds异常
     * 
     * @param offset
     * 
     * @param maxSize
     * @return
     * @throws IOException
     */
    public MessageSet slice(final long offset, final int maxSize) throws IOException {
        final Segment segment = this.findSegment(this.segments.view(), offset);
        if (segment == null) {
            return null;
        }
        else {
            return segment.fileMessageSet.slice(offset - segment.start, offset - segment.start + maxSize);
        }
    }


    /**
     * 返回离指定offset往前追溯最近的可用offset ,当传入的offset超出范围的时候返回边界offset
     * 
     * @param offset
     * @return
     */
    public long getNearestOffset(final long offset) {
        return this.getNearestOffset(offset, this.segments);
    }


    long getNearestOffset(final long offset, final SegmentList segments) {
        try {
            final Segment segment = this.findSegment(segments.view(), offset);
            if (segment != null) {
                return segment.start;
            }
            else {
                final Segment last = segments.last();
                return last.start + last.size();
            }
        }
        catch (final ArrayIndexOutOfBoundsException e) {
            return segments.first().start;
        }
    }


    /**
     * 根据offset查找文件,如果超过尾部，则返回null，如果在头部之前，则抛出ArrayIndexOutOfBoundsException
     * 
     * @param segments
     * @param offset
     * @return 返回找到segment，如果超过尾部，则返回null，如果在头部之前，则抛出异常
     * @throws ArrayIndexOutOfBoundsException
     */
    Segment findSegment(final Segment[] segments, final long offset) {
        if (segments == null || segments.length < 1) {
            return null;
        }
        // 老的数据不存在，返回最近最老的数据
        final Segment last = segments[segments.length - 1];
        // 在头部以前，抛出异常
        if (offset < segments[0].start) {
            throw new ArrayIndexOutOfBoundsException();
        }
        // 刚好在尾部或者超出范围，返回null
        if (offset >= last.start + last.size()) {
            return null;
        }
        // 根据offset二分查找
        int low = 0;
        int high = segments.length - 1;
        while (low <= high) {
            final int mid = high + low >>> 1;
        final Segment found = segments[mid];
        if (found.contains(offset)) {
            return found;
        }
        else if (offset < found.start) {
            high = mid - 1;
        }
        else {
            low = mid + 1;
        }
        }
        return null;
    }
}