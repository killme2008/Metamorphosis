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
package com.taobao.metamorphosis.client;

import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.RequestCommand;
import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.command.ResponseStatus;
import com.taobao.gecko.core.nio.impl.TimerRef;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.ConnectionLifeCycleListener;
import com.taobao.gecko.service.ConnectionSelector;
import com.taobao.gecko.service.GroupAllConnectionCallBackListener;
import com.taobao.gecko.service.MultiGroupCallBackListener;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.RemotingContext;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.config.ClientConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.network.BooleanCommand;


/**
 * RemotingClient包装，添加连接的建立和关闭的计数
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public class RemotingClientWrapper implements RemotingClient {
    private final RemotingClient remotingClient;
    private final ConcurrentHashMap<String/* url */, Set<Object>/* references */> refsCache =
            new ConcurrentHashMap<String, Set<Object>>();


    public RemotingClientWrapper(final RemotingClient remotingClient) {
        super();
        this.remotingClient = remotingClient;
    }


    @Override
    public void connect(String url, String targetGroup, int connCount) throws NotifyRemotingException {
        this.remotingClient.connect(url, targetGroup, connCount);

    }


    @Override
    public void connect(String url, String targetGroup) throws NotifyRemotingException {
        this.remotingClient.connect(url, targetGroup);
    }


    @Override
    public void addAllProcessors(
            final Map<Class<? extends RequestCommand>, RequestProcessor<? extends RequestCommand>> map) {
        this.remotingClient.addAllProcessors(map);
    }


    @Override
    public void addConnectionLifeCycleListener(final ConnectionLifeCycleListener connectionLifeCycleListener) {
        this.remotingClient.addConnectionLifeCycleListener(connectionLifeCycleListener);
    }


    @Override
    public void awaitReadyInterrupt(final String url, final long time) throws NotifyRemotingException,
    InterruptedException {
        this.remotingClient.awaitReadyInterrupt(url, time);
    }


    @Override
    public void awaitReadyInterrupt(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.awaitReadyInterrupt(url);
    }


    @Override
    public void connect(String url) throws NotifyRemotingException {
        this.connect(url, 1);
    }


    @Override
    public void connect(String url, int connCount) throws NotifyRemotingException {
        this.connectWithRef(url, connCount, null);
    }


    @Override
    public void close(String url, boolean allowReconnect) throws NotifyRemotingException {
        this.closeWithRef(url, null, allowReconnect);
    }


    public void closeWithRef(final String url, Object ref, final boolean allowReconnect) throws NotifyRemotingException {
        final Set<Object> refs = this.refsCache.get(url);
        if (refs != null) {
            synchronized (refs) {
                refs.remove(ref);
                if (refs.isEmpty()) {
                    this.remotingClient.close(url, allowReconnect);
                }
            }
        }
    }


    public void connectWithRef(final String url, final int connCount, Object ref) throws NotifyRemotingException {
        final Set<Object> refs = this.getReferences(url);
        synchronized (refs) {
            this.remotingClient.connect(url, connCount);
            refs.add(ref);
        }
    }


    private Set<Object> getReferences(final String url) {
        Set<Object> refs = this.refsCache.get(url);
        if (refs == null) {
            refs = new HashSet<Object>();
            final Set<Object> oldRefs = this.refsCache.putIfAbsent(url, refs);
            if (oldRefs != null) {
                refs = oldRefs;
            }
        }
        return refs;
    }


    public void connectWithRef(final String url, Object ref) throws NotifyRemotingException {
        this.connectWithRef(url, 1, ref);
    }


    @Override
    public Object getAttribute(final String group, final String key) {
        return this.remotingClient.getAttribute(group, key);
    }


    @Override
    public int getConnectionCount(final String group) {
        return this.remotingClient.getConnectionCount(group);
    }


    @Override
    public Set<String> getGroupSet() {
        return this.remotingClient.getGroupSet();
    }


    @Override
    public RequestProcessor<? extends RequestCommand> getProcessor(final Class<? extends RequestCommand> clazz) {
        return this.remotingClient.getProcessor(clazz);
    }


    @Override
    public InetSocketAddress getRemoteAddress(final String url) {
        return this.remotingClient.getRemoteAddress(url);
    }


    @Override
    public String getRemoteAddressString(final String url) {
        return this.remotingClient.getRemoteAddressString(url);
    }


    @Override
    public RemotingContext getRemotingContext() {
        return this.remotingClient.getRemotingContext();
    }


    @Override
    public void insertTimer(final TimerRef timerRef) {
        this.remotingClient.insertTimer(timerRef);
    }


    @Override
    public ResponseCommand invokeToGroup(final String group, final RequestCommand command, final long time,
            final TimeUnit timeUnit) throws InterruptedException, TimeoutException, NotifyRemotingException {
        ResponseCommand resp = this.remotingClient.invokeToGroup(group, command, time, timeUnit);
        if (resp.getResponseStatus() == ResponseStatus.ERROR_COMM) {
            BooleanCommand booleanCommand = (BooleanCommand) resp;
            if (booleanCommand.getErrorMsg().contains("无可用连接")) {
                // try to connect it.
                this.connectWithRef(group, this);
            }
        }
        return resp;
    }


    @Override
    public ResponseCommand invokeToGroup(final String group, final RequestCommand command) throws InterruptedException,
    TimeoutException, NotifyRemotingException {
        return this.remotingClient.invokeToGroup(group, command);
    }


    @Override
    public Map<Connection, ResponseCommand> invokeToGroupAllConnections(final String group,
        final RequestCommand command, final long time, final TimeUnit timeUnit) throws InterruptedException,
        NotifyRemotingException {
        return this.remotingClient.invokeToGroupAllConnections(group, command, time, timeUnit);
    }


    @Override
    public Map<Connection, ResponseCommand> invokeToGroupAllConnections(final String group, final RequestCommand command)
            throws InterruptedException, NotifyRemotingException {
        return this.remotingClient.invokeToGroupAllConnections(group, command);
    }


    @Override
    public boolean isConnected(final String url) {
        return this.remotingClient.isConnected(url);
    }


    @Override
    public boolean isStarted() {
        return this.remotingClient.isStarted();
    }


    @Override
    public <T extends RequestCommand> void registerProcessor(final Class<T> commandClazz,
            final RequestProcessor<T> processor) {
        this.remotingClient.registerProcessor(commandClazz, processor);
    }


    @Override
    public Object removeAttribute(final String group, final String key) {
        return this.remotingClient.removeAttribute(group, key);
    }


    @Override
    public void removeConnectionLifeCycleListener(final ConnectionLifeCycleListener connectionLifeCycleListener) {
        this.remotingClient.removeConnectionLifeCycleListener(connectionLifeCycleListener);
    }


    @Override
    public Connection selectConnectionForGroup(final String group, final ConnectionSelector connectionSelector,
            final RequestCommand request) throws NotifyRemotingException {
        return this.remotingClient.selectConnectionForGroup(group, connectionSelector, request);
    }


    @Override
    public void sendToAllConnections(final RequestCommand command) throws NotifyRemotingException {
        this.remotingClient.sendToAllConnections(command);
    }


    @Override
    public void sendToGroup(final String group, final RequestCommand command,
            final SingleRequestCallBackListener listener, final long time, final TimeUnit timeunut)
                    throws NotifyRemotingException {
        this.remotingClient.sendToGroup(group, command, listener, time, timeunut);
    }


    @Override
    public void sendToGroup(final String group, final RequestCommand command,
            final SingleRequestCallBackListener listener) throws NotifyRemotingException {
        this.remotingClient.sendToGroup(group, command, listener);
    }


    @Override
    public void sendToGroup(final String group, final RequestCommand command) throws NotifyRemotingException {
        this.remotingClient.sendToGroup(group, command);
    }


    @Override
    public void sendToGroupAllConnections(final String group, final RequestCommand command,
            final GroupAllConnectionCallBackListener listener, final long time, final TimeUnit timeUnit)
                    throws NotifyRemotingException {
        this.remotingClient.sendToGroupAllConnections(group, command, listener, time, timeUnit);
    }


    @Override
    public void sendToGroupAllConnections(final String group, final RequestCommand command,
            final GroupAllConnectionCallBackListener listener) throws NotifyRemotingException {
        this.remotingClient.sendToGroupAllConnections(group, command, listener);
    }


    @Override
    public void sendToGroupAllConnections(final String group, final RequestCommand command)
            throws NotifyRemotingException {
        this.remotingClient.sendToGroupAllConnections(group, command);
    }


    @Override
    public void sendToGroups(final Map<String, RequestCommand> groupObjects, final MultiGroupCallBackListener listener,
            final long timeout, final TimeUnit timeUnit, final Object... args) throws NotifyRemotingException {
        this.remotingClient.sendToGroups(groupObjects, listener, timeout, timeUnit, args);
    }


    @Override
    public void sendToGroups(final Map<String, RequestCommand> groupObjects) throws NotifyRemotingException {
        this.remotingClient.sendToGroups(groupObjects);
    }


    @Override
    public void setAttribute(final String group, final String key, final Object value) {
        this.remotingClient.setAttribute(group, key, value);
    }


    @Override
    public Object setAttributeIfAbsent(final String group, final String key, final Object value) {
        return this.remotingClient.setAttributeIfAbsent(group, key, value);
    }


    @Override
    public void setClientConfig(final ClientConfig clientConfig) {
        this.remotingClient.setClientConfig(clientConfig);
    }


    @Override
    public void setConnectionSelector(final ConnectionSelector selector) {
        this.remotingClient.setConnectionSelector(selector);
    }


    @Override
    public void start() throws NotifyRemotingException {
        this.remotingClient.start();
    }


    @Override
    public void stop() throws NotifyRemotingException {
        this.remotingClient.stop();
        this.refsCache.clear();
    }


    @Override
    public RequestProcessor<? extends RequestCommand> unreigsterProcessor(final Class<? extends RequestCommand> clazz) {
        return this.remotingClient.unreigsterProcessor(clazz);
    }


    @Override
    public void transferToGroup(String group, IoBuffer head, IoBuffer tail, FileChannel channel, long position,
            long size, Integer opaque, SingleRequestCallBackListener listener, long time, TimeUnit unit)
                    throws NotifyRemotingException {
        this.remotingClient.transferToGroup(group, head, tail, channel, position, size, opaque, listener, time, unit);

    }


    @Override
    public void transferToGroup(String group, IoBuffer head, IoBuffer tail, FileChannel channel, long position,
            long size) throws NotifyRemotingException {
        this.remotingClient.transferToGroup(group, head, tail, channel, position, size);
    }

}