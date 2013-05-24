/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.utils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 与zk交互的工具类
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-26
 */
public class ZkUtils {

    private static Log logger = LogFactory.getLog(ZkUtils.class);


    /**
     * make sure a persiste.nt path exists in ZK. Create the path if not exist.
     */
    public static void makeSurePersistentPathExists(final ZkClient client, final String path) throws Exception {
        if (!client.exists(path)) {
            try {
                client.createPersistent(path, true);
            }
            catch (final ZkNodeExistsException e) {
            }
            catch (final Exception e) {
                throw e;
            }

        }
    }


    /**
     * create the parent path
     */
    public static void createParentPath(final ZkClient client, final String path) throws Exception {
        final String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }


    /**
     * Create an ephemeral node with the given path and data. Create parents if
     * necessary.
     */
    public static void createEphemeralPath(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            client.createEphemeral(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }


    /**
     * Create an ephemeral node with the given path and data. Throw
     * NodeExistException if node already exists.
     */
    public static void createEphemeralPathExpectConflict(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            createEphemeralPath(client, path, data);
        }
        catch (final ZkNodeExistsException e) {

            // this canZkConfig happen when there is connection loss; make sure
            // the data
            // is what we intend to write
            String storedData = null;
            try {
                storedData = readData(client, path);
            }
            catch (final ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller
                // handles this
            }
            catch (final Exception e2) {
                throw e2;
            }
            if (storedData == null || !storedData.equals(data)) {
                throw e;
            }
            else {
                // otherwise, the creation succeeded, return normally
                logger.info(path + " exists with value " + data + " during connection loss; this is ok");
            }
        }
        catch (final Exception e) {
            throw e;
        }

    }


    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public static void updatePersistentPath(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            client.writeData(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
        catch (final Exception e) {
            throw e;
        }
    }


    public static String readData(final ZkClient client, final String path) {
        return client.readData(path);
    }


    public static String readDataMaybeNull(final ZkClient client, final String path) {
        return client.readData(path, true);
    }


    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public static void updateEphemeralPath(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            client.writeData(path, data);
        }
        catch (final ZkNoNodeException e) {

            createParentPath(client, path);
            client.createEphemeral(path, data);

        }
        catch (final Exception e) {
            throw e;
        }
    }


    public static void deletePath(final ZkClient client, final String path) throws Exception {
        try {
            client.delete(path);
        }
        catch (final ZkNoNodeException e) {
            logger.info(path + " deleted during connection loss; this is ok");
        }
        catch (final Exception e) {
            throw e;
        }
    }


    public static void deletePathRecursive(final ZkClient client, final String path) throws Exception {
        try {
            client.deleteRecursive(path);
        }
        catch (final ZkNoNodeException e) {
            logger.info(path + " deleted during connection loss; this is ok");

        }
        catch (final Exception e) {
            throw e;
        }
    }


    public static List<String> getChildren(final ZkClient client, final String path) {
        return client.getChildren(path);
    }


    public static List<String> getChildrenMaybeNull(final ZkClient client, final String path) {
        try {
            return client.getChildren(path);
        }
        catch (final ZkNoNodeException e) {
            return null;
        }
    }


    /**
     * Check if the given path exists
     */
    public static boolean pathExists(final ZkClient client, final String path) {
        return client.exists(path);
    }


    public static String getLastPart(final String path) {
        if (path == null) {
            return null;
        }
        final int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        }
        else {
            return null;
        }
    }

    public static class StringSerializer implements ZkSerializer {

        @Override
        public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
            try {
                return new String(bytes, "utf-8");
            }
            catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }


        @Override
        public byte[] serialize(final Object data) throws ZkMarshallingError {
            try {
                return ((String) data).getBytes("utf-8");
            }
            catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }

    }

    public static class ZKConfig extends Config implements Serializable {
        static final long serialVersionUID = -1L;

        @Key(name = "zk.zkRoot")
        public String zkRoot = "/meta";
        /**
         * If enable zookeeper
         */
        @Key(name = "zk.zkEnable")
        public boolean zkEnable = true;

        /** ZK host string */
        @Key(name = "zk.zkConnect")
        public String zkConnect;

        /** zookeeper session timeout */
        @Key(name = "zk.zkSessionTimeoutMs")
        public int zkSessionTimeoutMs = 30000;

        /**
         * the max time that the client waits to establish a connection to
         * zookeeper
         */
        @Key(name = "zk.zkConnectionTimeoutMs")
        public int zkConnectionTimeoutMs = 30000;

        /** how far a ZK follower can be behind a ZK leader */
        @Key(name = "zk.zkSyncTimeMs")
        public int zkSyncTimeMs = 5000;


        public ZKConfig(final String zkConnect, final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs,
                final int zkSyncTimeMs) {
            super();
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
        }


        public String getZkRoot() {
            return this.zkRoot;
        }


        public void setZkRoot(String zkRoot) {
            this.zkRoot = zkRoot;
        }


        public boolean isZkEnable() {
            return this.zkEnable;
        }


        public void setZkEnable(boolean zkEnable) {
            this.zkEnable = zkEnable;
        }


        public String getZkConnect() {
            return this.zkConnect;
        }


        public void setZkConnect(String zkConnect) {
            this.zkConnect = zkConnect;
        }


        public int getZkSessionTimeoutMs() {
            return this.zkSessionTimeoutMs;
        }


        public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        }


        public int getZkConnectionTimeoutMs() {
            return this.zkConnectionTimeoutMs;
        }


        public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
        }


        public int getZkSyncTimeMs() {
            return this.zkSyncTimeMs;
        }


        public void setZkSyncTimeMs(int zkSyncTimeMs) {
            this.zkSyncTimeMs = zkSyncTimeMs;
        }


        public ZKConfig() {
            super();
            this.zkConnect = "localhost:2181";
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.zkConnect == null ? 0 : this.zkConnect.hashCode());
            result = prime * result + this.zkConnectionTimeoutMs;
            result = prime * result + (this.zkEnable ? 1231 : 1237);
            result = prime * result + (this.zkRoot == null ? 0 : this.zkRoot.hashCode());
            result = prime * result + this.zkSessionTimeoutMs;
            result = prime * result + this.zkSyncTimeMs;
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
            ZKConfig other = (ZKConfig) obj;
            if (this.zkConnect == null) {
                if (other.zkConnect != null) {
                    return false;
                }
            }
            else if (!this.zkConnect.equals(other.zkConnect)) {
                return false;
            }
            if (this.zkConnectionTimeoutMs != other.zkConnectionTimeoutMs) {
                return false;
            }
            if (this.zkEnable != other.zkEnable) {
                return false;
            }
            if (this.zkRoot == null) {
                if (other.zkRoot != null) {
                    return false;
                }
            }
            else if (!this.zkRoot.equals(other.zkRoot)) {
                return false;
            }
            if (this.zkSessionTimeoutMs != other.zkSessionTimeoutMs) {
                return false;
            }
            if (this.zkSyncTimeMs != other.zkSyncTimeMs) {
                return false;
            }
            return true;
        }


        public ZKConfig(final String zkRoot, final String zkConnect, final int zkSessionTimeoutMs,
                final int zkConnectionTimeoutMs, final int zkSyncTimeMs, final boolean zkEnable) {
            super();
            this.zkRoot = zkRoot;
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
            this.zkEnable = zkEnable;
        }
    }

}