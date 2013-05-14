package com.taobao.metamorphosis.client;

import java.util.Iterator;
import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Allows browsing of messages on a Topic.
 * 
 * A client uses a TopicBrowser to look at messages on a topic without consuming
 * them.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public interface TopicBrowser {

    /**
     * Returns an iterator to iterate all messages under this topic from all
     * alive brokers.The iteration order is from the smallest broker's smallest
     * partition to the biggest broker's biggest partition.Everytime it returns
     * a new iterator.
     * 
     * @return
     */
    public Iterator<Message> iterator();


    /**
     * Close the topic browser.
     * 
     * @throws MetaClientException
     */
    public void close() throws MetaClientException;


    /**
     * Returns topic's all alive partitions.
     * 
     * @return
     */
    public List<Partition> getPartitions();


    /**
     * Returns the topic
     * 
     * @return
     */
    public String getTopic();
}
