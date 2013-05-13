package com.taobao.metamorphosis.gregor;

/**
 * Constant variables for gregor master/slave
 * 
 * @author apple
 * 
 */
public class Constants {

    /**
     * A topic to test if slave is ok.
     */
    public static String TEST_SLAVE_TOPIC = System.getProperty("metaq.slave.status.test.topic",
            "_metaq_slave_status_test");
}
