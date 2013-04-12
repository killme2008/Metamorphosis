package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Abstract partition selector.
 * 
 * @author apple
 * 
 */
public abstract class AbstractPartitionSelector implements PartitionSelector {

    @Override
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException {
        if (partitions == null) {
            throw new MetaClientException("There is no aviable partition for topic " + topic
                + ",maybe you don't publish it at first?");
        }
        return this.getPartition0(topic, partitions, message);
    }


    public abstract Partition getPartition0(String topic, List<Partition> partitions, Message message)
            throws MetaClientException;
}
