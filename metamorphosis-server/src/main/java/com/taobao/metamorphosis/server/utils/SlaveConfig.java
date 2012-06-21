package com.taobao.metamorphosis.server.utils;

import java.io.Serializable;

import com.taobao.metamorphosis.utils.Config;


/**
 * Async slave config
 * 
 * @author apple
 * 
 */
public class SlaveConfig extends Config implements Serializable {
    private static final long serialVersionUID = 1L;

    // slave编号,大于等于0表示作为slave启动
    private int slaveId = -1;
    // 作为slave启动时向master订阅消息的group,如果没配置则默认为meta-slave-group
    private String slaveGroup = "meta-slave-group";
    // slave数据同步的最大延时,单位毫秒
    private long slaveMaxDelayInMills = 500;
    // 是否自动从master同步server.ini
    // 第一次仍然需要自己拷贝server.ini，后续可以通过设置此选项为true来自动同步
    private boolean autoSyncMasterConfig;


    public SlaveConfig(int slaveId) {
        super();
        this.slaveId = slaveId;
    }


    public SlaveConfig(int slaveId, String slaveGroup, long slaveMaxDelayInMills, boolean syncMasterConfig) {
        super();
        this.slaveId = slaveId;
        this.slaveGroup = slaveGroup;
        this.slaveMaxDelayInMills = slaveMaxDelayInMills;
        this.autoSyncMasterConfig = syncMasterConfig;
    }


    public SlaveConfig() {
        super();
    }


    public int getSlaveId() {
        return this.slaveId;
    }


    public void setSlaveId(int slaveId) {
        this.slaveId = slaveId;
    }


    public String getSlaveGroup() {
        return this.slaveGroup;
    }


    public void setSlaveGroup(String slaveGroup) {
        this.slaveGroup = slaveGroup;
    }


    public long getSlaveMaxDelayInMills() {
        return this.slaveMaxDelayInMills;
    }


    public void setSlaveMaxDelayInMills(long slaveMaxDelayInMills) {
        this.slaveMaxDelayInMills = slaveMaxDelayInMills;
    }


    public boolean isAutoSyncMasterConfig() {
        return this.autoSyncMasterConfig;
    }


    public void setAutoSyncMasterConfig(boolean syncMasterConfig) {
        this.autoSyncMasterConfig = syncMasterConfig;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.slaveGroup == null ? 0 : this.slaveGroup.hashCode());
        result = prime * result + this.slaveId;
        result = prime * result + (int) (this.slaveMaxDelayInMills ^ this.slaveMaxDelayInMills >>> 32);
        result = prime * result + (this.autoSyncMasterConfig ? 1231 : 1237);
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
        SlaveConfig other = (SlaveConfig) obj;
        if (this.slaveGroup == null) {
            if (other.slaveGroup != null) {
                return false;
            }
        }
        else if (!this.slaveGroup.equals(other.slaveGroup)) {
            return false;
        }
        if (this.slaveId != other.slaveId) {
            return false;
        }
        if (this.slaveMaxDelayInMills != other.slaveMaxDelayInMills) {
            return false;
        }
        if (this.autoSyncMasterConfig != other.autoSyncMasterConfig) {
            return false;
        }
        return true;
    }

}
