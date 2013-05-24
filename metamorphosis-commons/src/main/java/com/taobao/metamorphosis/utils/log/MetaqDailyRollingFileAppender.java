package com.taobao.metamorphosis.utils.log;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;


/**
 * Custom daily rolling file appender for metaq,it keeps an bounded queue in
 * memory to hold latest logs.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class MetaqDailyRollingFileAppender extends DailyRollingFileAppender {

    private int logBufferSize = Integer.valueOf(System.getProperty(
        "metaq.log4j.daily.rolling.file.appender.log.bufferSize", "20"));

    private LinkedList<LoggingEvent> events;


    public List<String> getLogs(long timestamp) {
        List<String> rt = new ArrayList<String>();
        List<LoggingEvent> copiedEvents = this.getEvents();
        for (LoggingEvent event : copiedEvents) {
            if (event.timeStamp > timestamp) {
                rt.add(this.layout.format(event));
            }
        }
        return rt;
    }


    private List<LoggingEvent> getEvents() {
        List<LoggingEvent> copiedEvents;
        synchronized (this.events) {
            copiedEvents = new ArrayList<LoggingEvent>(this.events);
        }
        return copiedEvents;
    }


    public void setLogBufferSize(int logBufferSize) {
        if (logBufferSize <= 0) {
            throw new IllegalArgumentException("Invalid logBufferSize.");
        }
        this.logBufferSize = logBufferSize;
    }


    public int getLogBufferSize() {
        return this.logBufferSize;
    }


    @Override
    public void activateOptions() {
        super.activateOptions();
        this.events = new LinkedList<LoggingEvent>();
    }


    @Override
    protected void subAppend(LoggingEvent event) {
        synchronized (this.events) {
            while (this.events.size() >= this.logBufferSize) {
                this.events.poll();
            }
            this.events.offer(event);
        }
        super.subAppend(event);
    }

}
