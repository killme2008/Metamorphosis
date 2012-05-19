package com.taobao.metamorphosis.cluster.json;

import java.io.Serializable;

import com.taobao.metamorphosis.utils.JSONUtils;


/**
 * Topic's broker info to be registed in zookeeper.
 * 
 * @author dennis
 * @since 1.4.3
 * @date 2012-05-19
 * 
 */
public class TopicBroker implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private int numParts;

    private String broker;


    public TopicBroker() {
        super();
    }


    public TopicBroker(int numParts, String broker) {
        super();
        this.numParts = numParts;
        this.broker = broker;
    }


    public int getNumParts() {
        return this.numParts;
    }


    public void setNumParts(int numParts) {
        this.numParts = numParts;
    }


    public String getBroker() {
        return this.broker;
    }


    public void setBroker(String broker) {
        this.broker = broker;
    }


    public static TopicBroker parse(String json) throws Exception {
        return (TopicBroker) JSONUtils.deserializeObject(json, TopicBroker.class);
    }


    public String toJson() throws Exception {
        return JSONUtils.serializeObject(this);
    }


    @Override
    public String toString() {
        try {
            return this.toJson();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.broker == null ? 0 : this.broker.hashCode());
        result = prime * result + this.numParts;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        TopicBroker other = (TopicBroker) obj;
        if (this.broker == null) {
            if (other.broker != null) {
                return false;
            }
        }
        else if (!this.broker.equals(other.broker)) {
            return false;
        }
        if (this.numParts != other.numParts) {
            return false;
        }
        return true;
    }
}
