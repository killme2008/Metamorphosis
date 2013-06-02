package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;


/**
 * A bean factory to create an instance of MessageSessionFactory.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public class MetaqMessageSessionFactoryBean extends AbstractMetaqMessageSessionFactory<MessageSessionFactory> {

    @Override
    public MessageSessionFactory getObject() throws Exception {
        this.sessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
        return this.sessionFactory;
    }


    @Override
    public Class<?> getObjectType() {
        return MessageSessionFactory.class;
    }

}
