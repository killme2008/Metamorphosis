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
package com.taobao.metamorphosis.http.processor;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.network.PutCallback;


/**
 * 
 * Use Jetty as http server and handle request of get message/put message/get
 * offset
 * 
 */
public class MetamorphosisOnJettyProcessor extends AbstractHandler {
    private static final Log logger = LogFactory.getLog(MetamorphosisOnJettyProcessor.class);
    private final CommandProcessor commandProcessor;


    public MetamorphosisOnJettyProcessor(final MetaMorphosisBroker metaMorphosisBroker) {
        super();
        this.commandProcessor = metaMorphosisBroker.getBrokerProcessor();
    }


    @Override
    public void handle(final String target, final Request jettyRequest, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException, ServletException {
        if (target.length() < 2) {
            response.setStatus(HttpStatus.BadRequest);
            response.getWriter().write("Invalid request");
        }
        else {
            final char command = target.charAt(1);
            switch (command) {
            case 'g':
                this.getMessage(jettyRequest, response);
                break;
            case 'p':
                this.putMessage(jettyRequest, response);
                break;
            case 'o':
                this.getOffset(jettyRequest, response);
                break;
            default:
                response.setStatus(HttpStatus.BadRequest);
                response.getWriter().write("Invalid request");
                break;
            }
        }
        response.flushBuffer();
    }


    private void getOffset(final Request jettyRequest, final HttpServletResponse response) throws IOException {
        final String topic = jettyRequest.getParameter("topic");
        final int partition = Integer.parseInt(jettyRequest.getParameter("partition"));
        final String group = jettyRequest.getParameter("group");
        final long offset = Long.parseLong(jettyRequest.getParameter("offset"));

        try {
            final OffsetCommand offsetCommand = this.convert2OffsetCommand(topic, partition, group, offset);
            final ResponseCommand responseCommand = this.commandProcessor.processOffsetCommand(offsetCommand, null);
            response.setStatus(((BooleanCommand) responseCommand).getCode());
            response.getWriter().write(((BooleanCommand) responseCommand).getErrorMsg());
        }
        catch (final Throwable e) {
            logger.error("Could not get message from position " + offset, e);
            response.setStatus(HttpStatus.InternalServerError);
            response.getWriter().write(e.getMessage());
        }
    }


    private void putMessage(final Request jettyRequest, final HttpServletResponse response) throws IOException {
        final String topic = jettyRequest.getParameter("topic");
        try {
            // Validation should be done on client side already
            final int partition = Integer.parseInt(jettyRequest.getParameter("partition"));
            final int flag = Integer.parseInt(jettyRequest.getParameter("flag"));
            int checkSum = -1;
            if (StringUtils.isNotBlank(jettyRequest.getParameter("checksum"))) {
                checkSum = Integer.parseInt(jettyRequest.getParameter("checksum"));
            }
            // This stream should be handle by Jetty server and therefore it is
            // out of scope here without care close
            final InputStream inputStream = jettyRequest.getInputStream();
            final int dataLength = Integer.parseInt(jettyRequest.getParameter("length"));
            final byte[] data = new byte[dataLength];

            inputStream.read(data);
            this.doResponseHeaders(response, "text/plain");
            final PutCommand putCommand = this.convert2PutCommand(topic, partition, data, flag, checkSum);
            this.commandProcessor.processPutCommand(putCommand, null, new PutCallback() {

                @Override
                public void putComplete(final ResponseCommand resp) {
                    final BooleanCommand responseCommand = (BooleanCommand) resp;
                    response.setStatus(responseCommand.getCode());
                    try {
                        response.getWriter().write(responseCommand.getErrorMsg());
                    }
                    catch (final IOException e) {
                        logger.error("Write response failed", e);
                    }

                }
            });

        }
        catch (final Exception e) {
            logger.error("Put message failed", e);
            response.setStatus(HttpStatus.InternalServerError);
            response.getWriter().write(e.getMessage());
        }
    }


    private void getMessage(final Request jettyRequest, final HttpServletResponse response) throws IOException {
        // Validation should be done on client side already
        final String topic = jettyRequest.getParameter("topic");
        final int partition = Integer.parseInt(jettyRequest.getParameter("partition"));
        final String group = jettyRequest.getParameter("group");
        final long offset = Long.parseLong(jettyRequest.getParameter("offset"));
        final int maxSize = Integer.parseInt(jettyRequest.getParameter("maxsize"));

        try {
            final GetCommand getCommand = this.convert2GetCommand(topic, partition, group, offset, maxSize);
            final ResponseCommand responseCommand = this.commandProcessor.processGetCommand(getCommand, null, false);
            if (responseCommand instanceof DataCommand) {
                response.setStatus(HttpStatus.Success);
                response.getOutputStream().write(((DataCommand) responseCommand).getData());
            }
            else {
                response.setStatus(((BooleanCommand) responseCommand).getCode());
                response.getWriter().write(((BooleanCommand) responseCommand).getErrorMsg());
            }
        }
        catch (final Throwable e) {
            logger.error("Could not get message from position " + offset, e);
            response.setStatus(HttpStatus.InternalServerError);
            response.getWriter().write(e.getMessage());
        }
    }


    private GetCommand convert2GetCommand(final String topic, final int partition, final String group,
            final long offset, final int maxSize) {
        return new GetCommand(topic, group, partition, offset, maxSize, 0);
    }


    private OffsetCommand convert2OffsetCommand(final String topic, final int partition, final String group,
            final long offset) {
        return new OffsetCommand(topic, group, partition, offset, 0);
    }


    /**
     * Set the response headers. This method is called to set the response
     * headers such as content type and content length. May be extended to add
     * additional headers.
     * 
     * @param response
     * @param resource
     * @param mimeType
     */
    protected void doResponseHeaders(final HttpServletResponse response, final String mimeType) {
        if (mimeType != null) {
            response.setContentType(mimeType);
        }
    }


    private PutCommand convert2PutCommand(final String topic, final int partition, final byte[] data, final int flag,
            int checkSum) {
        return new PutCommand(topic, partition, data, null, flag, checkSum, 0);
    }

}