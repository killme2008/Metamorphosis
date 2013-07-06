package com.taobao.metamorphosis.server.filter;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.ThreadUtils;


/**
 * Consumer filter manager.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class ConsumerFilterManager implements Service {
    private ClassLoader filterClassLoader;
    private final ConcurrentHashMap<String/* class name */, FutureTask<ConsumerMessageFilter>> filters =
            new ConcurrentHashMap<String, FutureTask<ConsumerMessageFilter>>();
    private MetaConfig metaConfig;
    private static final Log log = LogFactory.getLog(ConsumerFilterManager.class);


    public ConsumerFilterManager() {

    }


    public ConsumerFilterManager(MetaConfig metaConfig) throws Exception {
        this.metaConfig = metaConfig;
        if (!StringUtils.isBlank(metaConfig.getAppClassPath())) {
            File dir = new File(metaConfig.getAppClassPath());
            File[] jars = dir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".jar");
                }
            });
            URL[] urls = new URL[jars.length + 1];
            urls[0] = dir.toURI().toURL();
            int i = 1;
            for (File jarFile : jars) {
                urls[i++] = jarFile.toURI().toURL();
            }
            this.filterClassLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        }
    }


    ClassLoader getFilterClassLoader() {
        return this.filterClassLoader;
    }


    void setFilterClassLoader(ClassLoader filterClassLoader) {
        this.filterClassLoader = filterClassLoader;
    }


    public ConsumerMessageFilter findFilter(final String topic, final String group) {
        if (this.filterClassLoader == null) {
            return null;
        }
        final String className = this.metaConfig.getTopicConfig(topic).getFilterClass(group);
        if (StringUtils.isBlank(className)) {
            return null;
        }
        FutureTask<ConsumerMessageFilter> task = this.filters.get(className);
        if (task == null) {
            task = new FutureTask<ConsumerMessageFilter>(new Callable<ConsumerMessageFilter>() {

                @Override
                public ConsumerMessageFilter call() throws Exception {
                    ConsumerMessageFilter intanceFilter = ConsumerFilterManager.this.intanceFilter(className);
                    if (intanceFilter != null) {
                        log.warn("Created filter '" + className + "' for group:" + group + " and topic:" + topic);
                    }
                    return intanceFilter;
                }

            });
            FutureTask<ConsumerMessageFilter> existsTask = this.filters.putIfAbsent(className, task);
            if (existsTask != null) {
                task = existsTask;
            }
            else {
                task.run();
            }
        }
        return this.getFilter0(task);
    }


    @SuppressWarnings("unchecked")
    private ConsumerMessageFilter intanceFilter(String className) throws Exception {
        Class<ConsumerMessageFilter> clazz =
                (Class<ConsumerMessageFilter>) Class.forName(className, true, this.filterClassLoader);
        if (clazz != null) {
            return clazz.newInstance();
        }
        else {
            return null;
        }
    }


    private ConsumerMessageFilter getFilter0(FutureTask<ConsumerMessageFilter> task) {
        try {
            return task.get();
        }
        catch (ExecutionException e) {
            throw ThreadUtils.launderThrowable(e.getCause());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }


    @Override
    public void init() {

    }


    @Override
    public void dispose() {
        this.filterClassLoader = null;
        this.filters.clear();

    }

}
