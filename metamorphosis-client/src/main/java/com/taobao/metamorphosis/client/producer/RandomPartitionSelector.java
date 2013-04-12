package com.taobao.metamorphosis.client.producer;

import java.util.List;
import java.util.Random;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Random partition selector
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class RandomPartitionSelector extends AbstractPartitionSelector {

    final Random rand = new Random();


    @Override
    public Partition getPartition0(String topic, List<Partition> partitions, Message message)
            throws MetaClientException {
        return partitions.get(this.rand.nextInt(partitions.size()));
    }

}
