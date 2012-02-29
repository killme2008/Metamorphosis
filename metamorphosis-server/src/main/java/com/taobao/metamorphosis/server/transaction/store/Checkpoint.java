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
package com.taobao.metamorphosis.server.transaction.store;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.server.utils.FileUtils;
import com.taobao.metamorphosis.utils.JSONUtils;


/**
 * checkpoint文件存储
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-24
 * 
 */
public class Checkpoint implements Closeable {
    private final Queue<DataFile> checkpoints;

    private final File transactionsDir;

    private DataFile currCheckpoint;

    private JournalLocation lastLocation;

    private int maxCheckpoints = 3;

    private final AtomicInteger number = new AtomicInteger(0);

    private static final String FILE_PREFIX = "checkpoint.";


    public Checkpoint(final String path, final int maxCheckpoints) throws Exception {
        FileUtils.makesureDir(new File(path));
        this.transactionsDir = new File(path + File.separator + "transactions");
        FileUtils.makesureDir(this.transactionsDir);
        this.maxCheckpoints = maxCheckpoints;
        this.checkpoints = new LinkedList<DataFile>();
        this.recover();
    }

    static final Log log = LogFactory.getLog(Checkpoint.class);


    private synchronized void recover() throws Exception {
        log.info("Begin to recover checkpoint journal...");
        final File[] ls = this.transactionsDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.startsWith(FILE_PREFIX);
            }
        });
        // 按照序号升序排序
        Arrays.sort(ls, new Comparator<File>() {

            @Override
            public int compare(final File o1, final File o2) {
                return Checkpoint.this.getFileNumber(o1) - Checkpoint.this.getFileNumber(o2);
            }

        });

        if (ls != null && ls.length > 0) {
            for (final File file : ls) {
                final DataFile df = new DataFile(file, this.getFileNumber(file), true);
                this.addCheckpoint(df);
            }
        }

        if (this.currCheckpoint == null) {
            this.newCheckpoint();
        }
        else {
            this.number.set(this.currCheckpoint.getNumber());
            this.lastLocation = this.readLocation(this.currCheckpoint);
        }
        log.info("Recover checkpoint journal succesfully");

    }


    synchronized List<DataFile> getCheckpoints() {
        return new ArrayList<DataFile>(this.checkpoints);
    }


    private JournalLocation readLocation(final DataFile df) throws Exception {
        if (df.getLength() == 0) {
            return null;
        }
        final ByteBuffer buf = ByteBuffer.allocate((int) df.getLength());
        df.read(buf, 0);
        buf.flip();
        final String json = ByteUtils.getString(buf.array());
        final JournalLocation rt = (JournalLocation) JSONUtils.deserializeObject(json, JournalLocation.class);
        return rt;
    }


    private void writeLocation(final DataFile df, final JournalLocation location) throws Exception {
        if (df == null || location == null) {
            return;
        }
        final String json = JSONUtils.serializeObject(location);
        final ByteBuffer buf = ByteBuffer.wrap(ByteUtils.getBytes(json));
        df.write(0, buf);
        df.force();
        df.truncate(buf.capacity());
    }


    /**
     * 返回最近的checkpoint
     * 
     * @return
     */
    public JournalLocation getRecentCheckpoint() {
        return this.lastLocation;
    }


    /**
     * 新设置checkpoint，如果没有改变则不存入磁盘，否则产生一个新的checkpoint文件
     * 
     * @param location
     */
    public synchronized void check(final JournalLocation location) throws Exception {
        if (location == null) {
            return;
        }
        if (this.lastLocation != null && this.lastLocation.compareTo(location) >= 0) {
            // 没有变化或者比前一个还老的直接返回
            return;
        }
        if (this.lastLocation != null) {
            this.currCheckpoint = this.newCheckpoint();
        }
        this.writeLocation(this.currCheckpoint, location);
        this.addCheckpoint(this.currCheckpoint);
        this.lastLocation = location;
    }


    private DataFile newCheckpoint() throws IOException {
        final int num = this.number.incrementAndGet();
        this.currCheckpoint =
                new DataFile(new File(this.transactionsDir + File.separator + FILE_PREFIX + num), num, true);
        return this.currCheckpoint;
    }


    private synchronized void addCheckpoint(final DataFile df) throws IOException {
        // 淘汰最老的
        while (this.checkpoints.size() >= this.maxCheckpoints) {
            final DataFile old = this.checkpoints.poll();
            if (old != null) {
                old.delete();
            }
        }
        this.checkpoints.add(df);
        this.currCheckpoint = df;
    }


    @Override
    public synchronized void close() throws IOException {
        for (final DataFile df : this.checkpoints) {
            df.close();
        }
    }


    private int getFileNumber(final File file) {
        final int number = Integer.parseInt(file.getName().substring(FILE_PREFIX.length()));
        return number;
    }
}