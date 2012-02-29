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
package com.taobao.metamorphosis.client.consumer.storage;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.JSONUtils;


/**
 * 本地offset存储，存储在磁盘，默认存储在$HOME/.meta_offsets文件中
 * 
 * @author boyan
 * @Date 2011-5-4
 * 
 */
public class LocalOffsetStorage implements OffsetStorage {
    private String filePath;

    static final Log log = LogFactory.getLog(LocalOffsetStorage.class);

    private final Map<String/* group */, List<TopicPartitionRegInfo>> groupInfoMap =
            new HashMap<String, List<TopicPartitionRegInfo>>();
    private final FileChannel channel;


    public LocalOffsetStorage() throws IOException {
        this(System.getProperty("user.home") + File.separator + ".meta_offsets");
    }


    public LocalOffsetStorage(final String filePath) throws IOException {
        final File file = new File(filePath);
        if (file.exists()) {
            this.loadGroupInfo(file);
        }
        else {
            file.createNewFile();
        }
        this.channel = new RandomAccessFile(file, "rw").getChannel();
    }


    private void loadGroupInfo(final File file) {
        String line = null;
        BufferedReader reader = null;
        FileReader fileReader = null;
        final StringBuilder jsonSB = new StringBuilder();
        try {
            fileReader = new FileReader(file);
            reader = new BufferedReader(fileReader);
            while ((line = reader.readLine()) != null) {
                jsonSB.append(line);
            }
        }
        catch (final IOException e) {
            log.error("读取文件" + file + "出错", e);
        }
        finally {
            this.close(reader);
            this.close(fileReader);
        }
        try {
            // 防止文件内容为空时将出现json反序列化异常,add by wuhua
            if (jsonSB.length() <= 0) {
                log.warn(file.getAbsolutePath() + "文件内容为空,暂时未加载到offset信息,如果是第一次发布这是正常现象");
                return;
            }

            final Map<String/* group */, List<Map<String, Object>>> groupInfoStringMap =
                    (Map<String/* group */, List<Map<String, Object>>>) JSONUtils.deserializeObject(jsonSB.toString(),
                        ConcurrentHashMap.class);

            for (final Map.Entry<String, List<Map<String, Object>>> entry1 : groupInfoStringMap.entrySet()) {
                final String group = entry1.getKey();
                final List<Map<String, Object>> infos = entry1.getValue();
                final List<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
                if (infos != null) {
                    for (final Map<String, Object> infoMap : infos) {
                        final String topic = (String) infoMap.get("topic");
                        final long offset = Long.valueOf(String.valueOf(infoMap.get("offset")));
                        final Map<String, Integer> partMap = (Map<String, Integer>) infoMap.get("partition");
                        infoList.add(new TopicPartitionRegInfo(topic, new Partition(partMap.get("brokerId"), partMap
                            .get("partition")), offset));
                    }
                }
                this.groupInfoMap.put(group, infoList);
            }
        }
        catch (final Exception e) {
            log.error("反序列化json失败", e);
        }
    }


    private void close(final Closeable closeable) {
        try {
            closeable.close();
        }
        catch (final IOException e) {
            // ignore
        }
    }


    @Override
    public void close() {
        this.close(this.channel);
    }


    @Override
    public void commitOffset(final String group, final Collection<TopicPartitionRegInfo> infoList) {
        if (infoList == null || infoList.isEmpty()) {
            return;
        }
        this.groupInfoMap.put(group, (List<TopicPartitionRegInfo>) infoList);
        try {
            final String json = JSONUtils.serializeObject(this.groupInfoMap);
            this.channel.position(0);
            final ByteBuffer buf = ByteBuffer.wrap(json.getBytes());
            while (buf.hasRemaining()) {
                this.channel.write(buf);
            }
            this.channel.truncate(this.channel.position());
        }
        catch (final Exception e) {
            log.error("commitOffset failed ", e);
        }

    }


    @Override
    public void initOffset(final String topic, final String group, final Partition partition, final long offset) {
        // do nothing
    }


    @Override
    public TopicPartitionRegInfo load(final String topic, final String group, final Partition partition) {
        final Collection<TopicPartitionRegInfo> topicPartitionRegInfos = this.groupInfoMap.get(group);
        if (topicPartitionRegInfos == null || topicPartitionRegInfos.isEmpty()) {
            return null;
        }
        for (final TopicPartitionRegInfo info : topicPartitionRegInfos) {
            if (info.getTopic().equals(topic) && info.getPartition().equals(partition)) {
                return info;
            }
        }
        return null;
    }

}