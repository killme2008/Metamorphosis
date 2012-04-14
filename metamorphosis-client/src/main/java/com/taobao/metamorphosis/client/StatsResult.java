package com.taobao.metamorphosis.client;

import java.util.HashMap;
import java.util.Map;


/**
 * Stats result
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public final class StatsResult {

    private Map<String/* key */, String/* stats value */> result = new HashMap<String, String>();


    public StatsResult(Map<String, String> result) {
        super();
        this.result = result;
    }


    /**
     * Get stats value by key
     * 
     * @param key
     * @return
     */
    public String getValue(String key) {
        return result.get(key);
    }


    /**
     * Get all stats values
     * 
     * @return
     */
    public Map<String/* key */, String/* stats value */> getAllValues() {
        return result;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.result == null ? 0 : this.result.hashCode());
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        StatsResult other = (StatsResult) obj;
        if (result == null) {
            if (other.result != null) {
                return false;
            }
        }
        else if (!result.equals(other.result)) {
            return false;
        }
        return true;
    }

}
