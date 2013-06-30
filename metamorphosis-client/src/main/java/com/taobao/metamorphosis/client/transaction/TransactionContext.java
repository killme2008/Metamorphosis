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
package com.taobao.metamorphosis.client.transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.command.ResponseStatus;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.TransactionInProgressException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.LongSequenceGenerator;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 事务上下文，同时支持本地事务和XA事务
 * 
 * @author boyan
 * 
 */
public class TransactionContext implements XAResource {

    private static final class EndXATransactionListener implements SingleRequestCallBackListener {
        @Override
        public void onResponse(ResponseCommand responseCommand, Connection conn) {

        }


        @Override
        public void onException(Exception e) {
            log.warn("End xa transaction failed:" + e.getMessage());
        }


        @Override
        public ThreadPoolExecutor getExecutor() {
            return null;
        }
    }

    public static final EndXATransactionListener END_XA_TX_LISTENER = new EndXATransactionListener();

    public static final Log log = LogFactory.getLog(TransactionContext.class);

    private final RemotingClient remotingClient;

    private Xid associatedXid;
    private String serverUrl;
    private TransactionId transactionId;
    private final String sessionId;
    private final LongSequenceGenerator localTransactionIdGenerator;

    private int transactionTimeout;

    private static final Log LOG = LogFactory.getLog(TransactionContext.class);

    private final TransactionSession associatedSession;

    private final long startMs;

    // Unique qualifier for XAResource.
    private String uniqueQualifier;

    // XAResource urls
    private String[] xareresourceURLs;

    private final long transactionRequestTimeoutInMills;


    public String[] getXareresourceURLs() {
        return this.xareresourceURLs;
    }


    public void setXareresourceURLs(String[] xareresourceURLs) {
        this.xareresourceURLs = xareresourceURLs;
    }


    public String getUniqueQualifier() {
        return this.uniqueQualifier;
    }


    public void setUniqueQualifier(String uniqueQualifier) {
        this.uniqueQualifier = uniqueQualifier;
    }


    public TransactionId getTransactionId() {
        return this.transactionId;
    }


    public void setServerUrl(final String serverUrl) {
        this.serverUrl = serverUrl;
    }


    public TransactionContext(final RemotingClient remotingClient, final String serverUrl,
            final TransactionSession session, final LongSequenceGenerator localTransactionIdGenerator,
            final int transactionTimeout, final long transactionRequestTimeoutInMills) {
        super();
        this.remotingClient = remotingClient;
        this.serverUrl = serverUrl;
        this.localTransactionIdGenerator = localTransactionIdGenerator;
        this.associatedSession = session;
        this.sessionId = session.getSessionId();
        this.transactionTimeout = transactionTimeout;
        this.transactionRequestTimeoutInMills = transactionRequestTimeoutInMills;
        this.startMs = System.currentTimeMillis();
    }


    private void logTxTime() {
        final long value = System.currentTimeMillis() - this.startMs;
        MetaStatLog.addStatValue2(null, StatConstants.TX_TIME, value);
    }


    public boolean isInXATransaction() {
        return this.transactionId != null && this.transactionId.isXATransaction();
    }


    public boolean isInLocalTransaction() {
        return this.transactionId != null && this.transactionId.isLocalTransaction();
    }


    public boolean isInTransaction() {
        return this.transactionId != null;
    }


    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Commit: " + xid);
        }

        XATransactionId x;
        if (xid == null || this.equals(this.associatedXid, xid)) {
            throw new XAException(XAException.XAER_PROTO);
        }
        else {
            x = new XATransactionId(xid, this.uniqueQualifier);
        }

        MetaStatLog.addStat(null, StatConstants.TX_COMMIT);
        this.checkConnectionConnected();
        try {

            // Notify the server that the tx was committed back
            final TransactionInfo info =
                    new TransactionInfo(x, this.sessionId, onePhase ? TransactionInfo.TransactionType.COMMIT_ONE_PHASE
                            : TransactionInfo.TransactionType.COMMIT_TWO_PHASE, this.uniqueQualifier);

            this.syncSendXATxCommand(info);
        }
        finally {
            this.associatedSession.removeContext(this);
            this.logTxTime();
        }

    }


    private void checkConnectionConnected() throws XAException {
        if (!this.remotingClient.isConnected(this.serverUrl)) {
            throw new XAException(XAException.XAER_RMFAIL);
        }
    }


    private boolean equals(final Xid xid1, final Xid xid2) {
        if (xid1 == xid2) {
            return true;
        }
        if (xid1 == null || xid2 == null) {
            return false;
        }
        return xid1.getFormatId() == xid2.getFormatId()
                && Arrays.equals(xid1.getBranchQualifier(), xid2.getBranchQualifier())
                && Arrays.equals(xid1.getGlobalTransactionId(), xid2.getGlobalTransactionId());
    }


    @Override
    public void end(final Xid xid, final int flags) throws XAException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("End: " + xid);
        }

        if (this.isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }

        if ((flags & (TMSUSPEND | TMFAIL)) != 0) {
            // You can only suspend the associated xid.
            if (!this.equals(this.associatedXid, xid)) {
                throw new XAException(XAException.XAER_PROTO);
            }

            // TODO implement resume?
            this.setXid(null);
        }
        else if ((flags & TMSUCCESS) == TMSUCCESS) {

            if (this.equals(this.associatedXid, xid)) {
                this.setXid(null);
            }
        }
        else {
            throw new XAException(XAException.XAER_INVAL);
        }

    }


    @Override
    public void forget(final Xid xid) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Forget: " + xid);
        }

        XATransactionId x;
        if (xid == null) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (this.equals(this.associatedXid, xid)) {
            x = (XATransactionId) this.transactionId;
        }
        else {
            x = new XATransactionId(xid, this.uniqueQualifier);
        }

        final TransactionInfo info =
                new TransactionInfo(x, this.sessionId, TransactionInfo.TransactionType.FORGET, this.uniqueQualifier);
        this.syncSendXATxCommand(info);
    }


    @Override
    public int getTransactionTimeout() throws XAException {
        return this.transactionTimeout;
    }


    private String getResourceManagerId() {
        return this.serverUrl;
    }


    @Override
    public boolean isSameRM(final XAResource xaResource) throws XAException {
        if (xaResource == null) {
            return false;
        }
        if (!(xaResource instanceof TransactionContext)) {
            return false;
        }
        final TransactionContext xar = (TransactionContext) xaResource;
        try {
            return this.getResourceManagerId().equals(xar.getResourceManagerId());
        }
        catch (final Throwable e) {
            throw (XAException) new XAException("Could not get resource manager id.").initCause(e);
        }
    }


    @Override
    public int prepare(final Xid xid) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Prepare: " + xid);
        }

        XATransactionId x;

        // 理论上不应该出现这种情况，因为end总是在prepare之前调用，associatedXid已经设置为null，预防
        if (xid == null || this.equals(this.associatedXid, xid)) {
            throw new XAException(XAException.XAER_PROTO);
        }
        else {
            x = new XATransactionId(xid, this.uniqueQualifier);
        }
        MetaStatLog.addStat(null, StatConstants.TX_PREPARE);

        final TransactionInfo info =
                new TransactionInfo(x, this.sessionId, TransactionInfo.TransactionType.PREPARE, this.uniqueQualifier);

        final BooleanCommand response = this.syncSendXATxCommand(info);
        final int result = Integer.parseInt(response.getErrorMsg());
        if (XAResource.XA_RDONLY == result) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("XA_RDONLY from prepare: " + xid);
            }
        }
        return result;

    }

    static final Pattern NEW_LINE_PATTERN = Pattern.compile("\r\n");

    static final Xid[] EMPTY_IDS = new Xid[0];


    @Override
    public Xid[] recover(final int flag) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Recover with flag: " + flag);
        }
        final TransactionInfo info =
                new TransactionInfo(null, this.sessionId, TransactionInfo.TransactionType.RECOVER, this.uniqueQualifier);
        try {
            final List<XATransactionId> xidList = new ArrayList<XATransactionId>();
            if (this.xareresourceURLs == null || this.xareresourceURLs.length == 0) {
                throw new XAException(XAException.XAER_RMFAIL);
            }
            // Recover from all XA resources.
            for (final String serverUrl : this.xareresourceURLs) {
                try {
                    final BooleanCommand receipt =
                            (BooleanCommand) this.remotingClient.invokeToGroup(serverUrl, new TransactionCommand(info,
                                OpaqueGenerator.getNextOpaque()), this.transactionRequestTimeoutInMills,
                                TimeUnit.MILLISECONDS);
                    if (receipt.getCode() != HttpStatus.Success) {
                        log.warn("Recover XAResource(" + serverUrl + ") failed,error message:" + receipt.getErrorMsg());
                        continue;
                    }
                    final String data = receipt.getErrorMsg();
                    if (StringUtils.isBlank(data)) {
                        continue;
                    }
                    final String[] xidStrs = NEW_LINE_PATTERN.split(data);
                    for (final String key : xidStrs) {
                        if (!StringUtils.isBlank(key)) {
                            xidList.add(new XATransactionId(key));
                        }
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw this.toXAException(e);
                }
                catch (Exception e) {
                    log.error("Recover XAResource(" + serverUrl + ") faile", e);
                }
            }
            if (xidList.isEmpty()) {
                return EMPTY_IDS;
            }
            else {
                Xid[] rt = new Xid[xidList.size()];
                return xidList.toArray(rt);
            }
        }
        catch (final Exception e) {
            throw this.toXAException(e);
        }
    }


    @Override
    public void rollback(final Xid xid) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Rollback: " + xid);
        }

        XATransactionId x;
        if (xid == null) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (this.equals(this.associatedXid, xid)) {
            x = (XATransactionId) this.transactionId;
        }
        else {
            x = new XATransactionId(xid, this.uniqueQualifier);
        }
        MetaStatLog.addStat(null, StatConstants.TX_ROLLBACK);
        this.checkConnectionConnected();
        try {

            final TransactionInfo info =
                    new TransactionInfo(x, this.sessionId, TransactionInfo.TransactionType.ROLLBACK,
                        this.uniqueQualifier);
            this.syncSendXATxCommand(info);
        }
        finally {
            this.associatedSession.removeContext(this);
            this.logTxTime();
        }

    }

    static Pattern EXCEPTION_PAT = Pattern.compile("code=(-?\\d+),msg=(.*)");


    private BooleanCommand syncSendXATxCommand(final TransactionInfo info) throws XAException {
        try {
            final BooleanCommand resp =
                    (BooleanCommand) this.remotingClient.invokeToGroup(this.serverUrl, new TransactionCommand(info,
                        OpaqueGenerator.getNextOpaque()), this.transactionRequestTimeoutInMills, TimeUnit.MILLISECONDS);
            if (resp.getResponseStatus() != ResponseStatus.NO_ERROR) {
                final String msg = resp.getErrorMsg();
                if (msg.startsWith("XAException:")) {
                    final Matcher m = EXCEPTION_PAT.matcher(msg);
                    if (m.find()) {
                        final int code = Integer.parseInt(m.group(1));
                        final String error = m.group(2);
                        final XAException xaException = new XAException(error);
                        xaException.errorCode = code;
                        throw xaException;
                    }
                    else {
                        this.throwRMFailException(resp);
                    }

                }
                else {
                    this.throwRMFailException(resp);
                }
            }
            return resp;
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw this.toXAException(e);
        }
        catch (final Exception e) {
            throw this.toXAException(e);
        }
    }


    private void throwRMFailException(final BooleanCommand resp) throws XAException {
        final XAException xaException = new XAException(resp.getErrorMsg());
        xaException.errorCode = XAException.XAER_RMERR;
        throw xaException;
    }


    @Override
    public boolean setTransactionTimeout(final int seconds) throws XAException {
        if (seconds < 0) {
            throw new XAException(XAException.XAER_INVAL);
        }
        this.transactionTimeout = seconds;
        return true;
    }


    /**
     * 将异常转化为XA异常
     * 
     * @param e
     * @return
     */
    XAException toXAException(final Exception e) {
        if (e instanceof TimeoutException) {
            final XAException xae = new XAException(e.getMessage());
            xae.errorCode = XAException.XA_RBTIMEOUT;
            xae.initCause(e);
            return xae;
        }
        if (e.getCause() != null && e.getCause() instanceof XAException) {
            final XAException original = (XAException) e.getCause();
            final XAException xae = new XAException(original.getMessage());
            xae.errorCode = original.errorCode;
            xae.initCause(original);
            return xae;
        }

        if (e instanceof XAException) {
            // ((XAException) e).errorCode = XAException.XAER_RMFAIL;
            return (XAException) e;
        }

        final XAException xae = new XAException(e.getMessage());
        xae.errorCode = XAException.XAER_RMFAIL;
        xae.initCause(e);
        return xae;
    }


    private void setXid(final Xid xid) throws XAException {
        this.checkConnectionConnected();
        if (xid != null) {
            this.startXATransaction(xid);
        }
        else {
            this.endXATransaction();
        }
    }


    private void endXATransaction() throws XAException {
        if (this.transactionId != null) {
            MetaStatLog.addStat(null, StatConstants.TX_END);
            final TransactionInfo info =
                    new TransactionInfo(this.transactionId, this.sessionId, TransactionInfo.TransactionType.END,
                        this.uniqueQualifier);
            try {
                this.remotingClient.sendToGroup(this.serverUrl,
                    new TransactionCommand(info, OpaqueGenerator.getNextOpaque()), END_XA_TX_LISTENER,
                    this.transactionRequestTimeoutInMills, TimeUnit.MILLISECONDS);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ended XA transaction: " + this.transactionId);
                }
            }
            catch (final Exception e) {
                throw this.toXAException(e);
            }
        }

        this.associatedXid = null;
        this.transactionId = null;
    }


    private void startXATransaction(final Xid xid) throws XAException {
        MetaStatLog.addStat(null, StatConstants.TX_BEGIN);
        this.associatedXid = xid;
        this.transactionId = new XATransactionId(xid, this.uniqueQualifier);

        final TransactionInfo info =
                new TransactionInfo(this.transactionId, this.sessionId, TransactionInfo.TransactionType.BEGIN,
                    this.uniqueQualifier, this.transactionTimeout);
        this.syncSendXATxCommand(info);
    }


    @Override
    public void start(final Xid xid, final int flags) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start: " + xid);
        }

        if ((flags & TMJOIN) == TMJOIN || (flags & TMRESUME) == TMRESUME) {
            // 加入或者resume的xid，必须与当前xid一样。
            if (!this.equals(this.associatedXid, xid)) {
                throw new XAException(XAException.XAER_NOTA);
            }
        }

        if (this.isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (this.associatedXid != null) {
            throw new XAException(XAException.XAER_DUPID);
        }
        this.setXid(xid);
    }


    /**
     * 下列方法为本地事务实现，begin,commit和rollback
     */

    public void begin() throws MetaClientException {

        if (this.isInXATransaction()) {
            throw new TransactionInProgressException(
                    "Cannot start local transaction.  XA transaction is already in progress.");
        }

        if (this.transactionId == null) {
            MetaStatLog.addStat(null, StatConstants.TX_BEGIN);
            this.transactionId =
                    new LocalTransactionId(this.sessionId, this.localTransactionIdGenerator.getNextSequenceId());
            // Local transaction doesn't need unique qualifier.
            final TransactionInfo info =
                    new TransactionInfo(this.transactionId, this.sessionId, TransactionInfo.TransactionType.BEGIN,
                        this.uniqueQualifier, this.transactionTimeout);
            try {
                this.checkConnectionConnected();
                this.syncSendLocalTxCommand(info);
            }
            catch (final Exception e) {
                throw this.toMetaClientException(e);
            }
        }
    }


    private MetaClientException toMetaClientException(final Exception e) {
        if (e instanceof MetaClientException) {
            return (MetaClientException) e;
        }
        else if (e instanceof XAException) {
            return new MetaClientException(e.getMessage(), e.getCause() != null ? e.getCause() : e);
        }
        return new MetaClientException(e);
    }


    public void commit() throws MetaClientException {
        if (this.isInXATransaction()) {
            throw new TransactionInProgressException("Cannot commit() if an XA transaction is already in progress ");
        }

        // Only send commit command if the transaction was started.
        if (this.transactionId != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Commit: " + this.transactionId);

            }
            MetaStatLog.addStat(null, StatConstants.TX_COMMIT);
            try {
                // Local transaction doesn't need unique qualifier.
                final TransactionInfo info =
                        new TransactionInfo(this.transactionId, this.sessionId,
                            TransactionInfo.TransactionType.COMMIT_ONE_PHASE, this.uniqueQualifier);
                this.transactionId = null;
                this.syncSendLocalTxCommand(info);
            }
            catch (TimeoutException e) {
                throw new MetaClientException(
                    "Commit transaction timeout,the transaction state is unknown,you must check it by yourself.", e);
            }
            finally {
                this.logTxTime();
            }
        }
        else {
            throw new MetaClientException("No transaction is started");
        }
    }


    @Override
    public String toString() {
        return this.serverUrl;
    }


    private void syncSendLocalTxCommand(final TransactionInfo info) throws MetaClientException, TimeoutException {
        try {

            final BooleanCommand resp =
                    (BooleanCommand) this.remotingClient.invokeToGroup(this.serverUrl, new TransactionCommand(info,
                        OpaqueGenerator.getNextOpaque()), this.transactionRequestTimeoutInMills, TimeUnit.MILLISECONDS);
            if (resp.getResponseStatus() != ResponseStatus.NO_ERROR) {
                throw new MetaClientException(resp.getErrorMsg());
            }
        }
        catch (TimeoutException te) {
            throw te;
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw this.toMetaClientException(e);
        }
        catch (final Exception e) {
            throw this.toMetaClientException(e);
        }
    }


    public void rollback() throws MetaClientException {
        if (this.isInXATransaction()) {
            throw new TransactionInProgressException("Cannot rollback() if an XA transaction is already in progress ");
        }

        if (this.transactionId != null) {
            MetaStatLog.addStat(null, StatConstants.TX_ROLLBACK);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rollback: " + this.transactionId);
            }
            try {
                final TransactionInfo info =
                        new TransactionInfo(this.transactionId, this.sessionId,
                            TransactionInfo.TransactionType.ROLLBACK, this.uniqueQualifier);
                this.transactionId = null;
                this.syncSendLocalTxCommand(info);
            }
            catch (TimeoutException e) {
                throw new MetaClientException(
                    "Rollback transaction timeout,the transaction state is unknown,you must check it by yourself.", e);
            }
            finally {
                this.logTxTime();
            }
        }
        else {
            throw new MetaClientException("No transaction is started");
        }

    }
}