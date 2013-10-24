package com.taobao.metamorphosis.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * TopicBrowser implementation.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 */
public class MetaTopicBrowser implements TopicBrowser {

    private final String topic;

    private final MessageConsumer consumer;

    private final List<Partition> partitions;

    private final int maxSize;

    private final long timeoutInMills;

    protected class Itr implements Iterator<Message> {
        protected final List<Partition> partitions;

        private MessageIterator it;

        private long offset = 0L;

        private Partition partition;


        public Itr(List<Partition> partitions) {
            super();
            this.partitions = partitions;
        }


        @Override
        public boolean hasNext() {
            try {
                if (this.it != null && this.it.hasNext()) {
                    return true;
                }
                else {
                    if (this.partition == null) {
                        if (this.partitions.isEmpty()) {
                            return false;
                        }
                        else {
                            this.nextPartition();
                        }
                    }
                    while (this.partition != null) {
                        // increase offset for this partition if it is not null.
                        if (this.it != null) {
                            this.offset += this.it.getOffset();
                        }
                        this.it =
                                MetaTopicBrowser.this.consumer.get(MetaTopicBrowser.this.topic, this.partition,
                                    this.offset, MetaTopicBrowser.this.maxSize, MetaTopicBrowser.this.timeoutInMills,
                                    TimeUnit.MILLISECONDS);
                        if (this.it != null && this.it.hasNext()) {
                            // If this partition still has messages, returns
                            // true.
                            return true;
                        }
                        else {
                            // move on to next partition.
                            if (this.partitions.isEmpty()) {
                                this.partition = null;
                                // There is no more partitions,return false.
                                return false;
                            }
                            else {
                                // Change partition,and continue trying to get
                                // iterator.
                                this.nextPartition();
                            }
                        }
                    }
                    return false;

                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }


        private void nextPartition() {
            this.partition = this.partitions.get(0);
            this.partitions.remove(0);
            this.offset = 0;
            this.it = null;
        }


        @Override
        public Message next() {
            if (this.hasNext()) {
                try {
                    return this.it.next();
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            else {
                throw new NoSuchElementException();
            }
        }


        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }


    public MessageConsumer getConsumer() {
        return this.consumer;
    }


    public MetaTopicBrowser(String topic, int maxSize, long timeoutInMills, MessageConsumer consumer,
            List<Partition> partitions) {
        super();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Invalid max size");
        }
        if (timeoutInMills <= 0) {
            throw new IllegalArgumentException("Invalid timeout value");
        }
        this.timeoutInMills = timeoutInMills;
        this.topic = topic;
        this.maxSize = maxSize;
        this.consumer = consumer;
        this.partitions = partitions;
    }


    @Override
    public Iterator<Message> iterator() {
        return new Itr(new ArrayList<Partition>(this.partitions));
    }


    @Override
    public List<Partition> getPartitions() {
        return Collections.unmodifiableList(this.partitions);
    }


    @Override
    public void shutdown() throws MetaClientException {
        this.consumer.shutdown();
    }


    @Override
    public String getTopic() {
        return this.topic;
    }

}
