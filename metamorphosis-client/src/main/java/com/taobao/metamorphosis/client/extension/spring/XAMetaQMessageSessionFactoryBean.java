package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;


/**
 * A bean factory to create an instance of XAMessageSessionFactory.
 * 
 * @since 1.4.5
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class XAMetaQMessageSessionFactoryBean extends MetaQMessageSessionFactoryBean {
    @Override
    public MessageSessionFactory getObject() throws Exception {
        this.sessionFactory = new XAMetaMessageSessionFactory(this.metaClientConfig);
        return this.sessionFactory;
    }


    @Override
    public Class<?> getObjectType() {
        return XAMessageSessionFactory.class;
    }

}
