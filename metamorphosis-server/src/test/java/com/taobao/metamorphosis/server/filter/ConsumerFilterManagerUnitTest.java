package com.taobao.metamorphosis.server.filter;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;


public class ConsumerFilterManagerUnitTest {

    private ConsumerFilterManager consumerFilterManager;


    @Before
    public void setUp() throws Exception {
        MetaConfig metaConfig = new MetaConfig();
        TopicConfig topicConfig = metaConfig.getTopicConfig("test");
        topicConfig.addFilterClass("test-group1", "com.taobao.metamorphosis.server.filter.NotExistsFilter");
        topicConfig.addFilterClass("test-group2", "com.taobao.metamorphosis.server.filter.TestFilter");
        this.consumerFilterManager = new ConsumerFilterManager(metaConfig);
    }


    @Test
    public void testGetNullFilter() {
        assertNull(this.consumerFilterManager.findFilter("test", "not-exists"));
    }


    @Test
    public void testGetNullFilterWithClassLoader() {
        this.consumerFilterManager.setFilterClassLoader(Thread.currentThread().getContextClassLoader());
        assertNull(this.consumerFilterManager.findFilter("test", "not-exists"));
    }


    @Test(expected = IllegalStateException.class)
    public void testGetFilterNotFound() {
        this.consumerFilterManager.setFilterClassLoader(Thread.currentThread().getContextClassLoader());
        assertNull(this.consumerFilterManager.findFilter("test", "test-group1"));
    }


    @Test
    public void testGetFilter() {
        this.consumerFilterManager.setFilterClassLoader(Thread.currentThread().getContextClassLoader());
        ConsumerMessageFilter filter = this.consumerFilterManager.findFilter("test", "test-group2");
        assertNotNull(filter);
        assertSame(filter, this.consumerFilterManager.findFilter("test", "test-group2"));
        assertTrue(filter.accept(null, null));
        assertTrue(filter.accept(null, null));
    }
}
