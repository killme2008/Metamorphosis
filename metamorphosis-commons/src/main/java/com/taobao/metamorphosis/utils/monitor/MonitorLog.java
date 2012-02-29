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
package com.taobao.metamorphosis.utils.monitor;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


/**
 * 应用埋点客户调用接口
 * 
 * @author mengting 2008-11-14
 * @modify fangliang 2010-02-22
 * @desc 例子请参见 MonitorLogExample.java
 */
public class MonitorLog {

    private static Logger logger = Logger.getLogger(MonitorLog.class);
    private static Logger appStatLog;
    private static Logger middlewareStatLog;

    private static final Map<Keys, ValueObject> appDatas = new ConcurrentHashMap<Keys, ValueObject>(100);
    private static final Map<Keys, ValueObject> middlewareDatas = new ConcurrentHashMap<Keys, ValueObject>(100);

    private static final ReentrantLock lock = new ReentrantLock();
    private static final ReentrantLock middlewareLock = new ReentrantLock();
    private static final ReentrantLock timerLock = new ReentrantLock();
    private static final Condition condition = timerLock.newCondition();

    /** JVM监控信息 */
    // private static final MemoryMXBean memoryMXBean =
    // ManagementFactory.getMemoryMXBean();
    // private static final ThreadMXBean threadMXBean =
    // ManagementFactory.getThreadMXBean();

    private static String hostName = "";
    private static String appName = null;

    /** MILLISECONDS static long waitTime = 120000L; */
    private static long waitTime = 120L;

    private static boolean jvmInfoPower = true;

    private static boolean writeLog = true;

    public static int appMaxKey = 100000;

    public static int middlewareMaxKey = 100000;

    /** 写入数据的线程 */
    private static Thread writeThread = null;

    /**
     * <p>
     * 日志文件的绝对路径
     * </p>
     */
    private static String logFileAbsolutePath = "";


    private MonitorLog() {
    }

    static {
        /** 动态创建日志记录的配置 */
        String userHome = System.getProperty(MonitorConstants.USER_HOME);
        if (!userHome.endsWith(File.separator)) {
            userHome += File.separator;
        }
        final String filePath = userHome + MonitorConstants.DIR_NAME + File.separator;
        final File dir = new File(filePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        // if(Thread.currentThread().getContextClassLoader() == null){
        // Thread.currentThread().setContextClassLoader(MonitorLog.class.getClassLoader());
        // }
        String classLoader = MonitorLog.class.getClassLoader().toString();
        classLoader = classLoader.split("@")[0];

        initAppLog4j(filePath, classLoader);
        initMiddlewareLog4j(filePath, classLoader);
        setHostName();

        runWriteThread();
        registerSelf();
    }


    /**
     * <p>
     * 取得默认的MBean Server
     * </p>
     */
    // private static MBeanServer mbs =
    // ManagementFactory.getPlatformMBeanServer();

    /**
     * 设置hostName
     */
    public static void setHostName() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            hostName = addr.getHostName();
        }
        catch (final UnknownHostException e) {
            logger.error("MonitorLog getLocalHost error", e);
        }
    }


    /**
     * 初始化应用的日志记录器
     * 
     * @param filePath
     * @param classLoader
     */
    private static void initAppLog4j(final String filePath, final String classLoader) {
        final String appFileName =
                filePath + MonitorConstants.APP_FILE_NAME + classLoader + MonitorConstants.FILE_SUFFIX;
        logFileAbsolutePath = appFileName;

        appStatLog = Logger.getLogger("app-" + classLoader);
        final PatternLayout layout = new PatternLayout(MonitorConstants.M);
        FileAppender appender = null;
        try {
            appender = new DailyRollingFileAppender(layout, appFileName, MonitorConstants.YYYY_MM_DD);
            appender.setAppend(true);
            appender.setEncoding(MonitorConstants.GBK);
        }
        catch (final IOException e) {
            logger.error("MonitorLog initLog4j error", e);
        }
        if (appender != null) {
            appStatLog.removeAllAppenders();
            appStatLog.addAppender(appender);
        }
        appStatLog.setLevel(Level.INFO);
        appStatLog.setAdditivity(false);
    }


    /**
     * 初始化中间件的日志记录器
     * 
     * @param filePath
     * @param classLoader
     */
    private static void initMiddlewareLog4j(final String filePath, final String classLoader) {
        final String middlewareFileName =
                filePath + MonitorConstants.MIDDLEWARE_FILE_NAME + classLoader + MonitorConstants.FILE_SUFFIX;
        middlewareStatLog = Logger.getLogger("middleware-" + classLoader);
        final PatternLayout layout = new PatternLayout(MonitorConstants.M);
        FileAppender appender = null;
        try {
            appender = new DailyRollingFileAppender(layout, middlewareFileName, MonitorConstants.YYYY_MM_DD);
            appender.setAppend(true);
            appender.setEncoding(MonitorConstants.GBK);
        }
        catch (final IOException e) {
            logger.error("MonitorLog initMiddlewareLog4j error", e);
        }
        if (appender != null) {
            middlewareStatLog.removeAllAppenders();
            middlewareStatLog.addAppender(appender);
        }
        middlewareStatLog.setLevel(Level.INFO);
        middlewareStatLog.setAdditivity(false);
    }


    /**
     * 执行写入线程
     */
    public static void runWriteThread() {
        if (null != writeThread) { // 如果线程还存在，先interrupt一下
            try {
                writeThread.interrupt();
            }
            catch (final Exception e) {
                logger.error("interrupt write thread error", e);
            }
        }
        // 初始化线程
        writeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 等待waitTime秒
                while (true) {
                    timerLock.lock();
                    try {
                        condition.await(waitTime, TimeUnit.SECONDS);
                    }
                    catch (final Exception e) {
                        logger.error("wait error", e);
                    }
                    finally {
                        timerLock.unlock();
                    }
                    MonitorLog.writeLog();
                }
            }
        });
        writeThread.setName(MonitorConstants.WRITETHREAD_NAME);
        writeThread.start();
    }


    public static String getLogFileAbsolutePath() {
        return logFileAbsolutePath;
    }


    /**
     * 写入日志的主方法
     */
    private static void writeLog() {
        // 写入一般应用
        final Map<Keys, ValueObject> tmp = new HashMap<Keys, ValueObject>(appDatas.size()); // 设置一个临时的数据是为了保证数据都写到文件后才减少datas的数据
        final StringBuilder sb = new StringBuilder();
        final SimpleDateFormat dateFormat = new SimpleDateFormat(MonitorConstants.DATE_FORMAT);
        final String appTime = dateFormat.format(Calendar.getInstance().getTime());
        for (final Keys key : appDatas.keySet()) {
            final long[] values = appDatas.get(key).getValues();
            final long value1 = values[0];
            final long value2 = values[1];
            if (0 == value1 && 0 == value2) {
                continue;
            }

            sb.append(key.getString(MonitorConstants.SPLITTER_1)).append(MonitorConstants.SPLITTER_1);
            sb.append(value1).append(MonitorConstants.SPLITTER_1);
            sb.append(value2).append(MonitorConstants.SPLITTER_1);
            sb.append(appTime).append(MonitorConstants.SPLITTER_1);
            sb.append(hostName).append(MonitorConstants.SPLITTER_2);
            tmp.put(key, new ValueObject(value1, value2));
        }

        // if (jvmInfoPower) {
        // sb.append(getJvmInfo(appTime));
        // }
        if (tmp.size() > 0 && writeLog) {
            appStatLog.info(sb);
        }

        // 循环把已经写入文件的数据从datas中减少
        for (final Keys key : tmp.keySet()) {
            final long[] values = tmp.get(key).getValues();
            appDatas.get(key).deductCount(values[0], values[1]);
        }

        final Map<Keys, ValueObject> middleTmp = new HashMap<Keys, ValueObject>(middlewareDatas.size()); // 设置一个临时的数据是为了保证数据都写到文件后才减少datas的数据
        final StringBuilder middleSb = new StringBuilder();

        final String middlewareTime = dateFormat.format(Calendar.getInstance().getTime());
        for (final Keys key : middlewareDatas.keySet()) {
            final long[] values = middlewareDatas.get(key).getValues();
            final long value1 = values[0];
            final long value2 = values[1];
            if (0 == value1 && 0 == value2) {
                continue;
            }

            middleSb.append(key.getString(MonitorConstants.SPLITTER_1)).append(MonitorConstants.SPLITTER_1);
            middleSb.append(value1).append(MonitorConstants.SPLITTER_1);
            middleSb.append(value2).append(MonitorConstants.SPLITTER_1);
            middleSb.append(middlewareTime).append(MonitorConstants.SPLITTER_1);
            middleSb.append(hostName).append(MonitorConstants.SPLITTER_2);
            middleTmp.put(key, new ValueObject(value1, value2));
        }

        if (middleTmp.size() > 0 && writeLog) {
            middlewareStatLog.info(middleSb);
        }

        // 循环把已经写入文件的数据从datas中减少
        for (final Keys key : middleTmp.keySet()) {
            final long[] values = middleTmp.get(key).getValues();
            middlewareDatas.get(key).deductCount(values[0], values[1]);
        }
    }


    /**
     * keyOne,keyTwo,keyThree不能带有类似\n的回行
     * 
     * @param keyOne
     * @param keyTwo
     * @param keyThree
     */
    public static void addStat(final String keyOne, final String keyTwo, final String keyThree) {
        final Keys keys = new Keys(keyOne, keyTwo, keyThree);
        addStat(keys, 1, 0);
    }


    /**
     * keyOne,keyTwo,keyThree不能带有类似\n的回行
     * 
     * @param keyOne
     * @param keyTwo
     * @param keyThree
     * @param value
     */
    public static void addStatValue2(final String keyOne, final String keyTwo, final String keyThree, final long value) {
        final Keys keys = new Keys(keyOne, keyTwo, keyThree);
        addStat(keys, 1, value);
    }


    /**
     * keyOne,keyTwo,keyThree不能带有类似\n的回行
     * 
     * @param keyOne
     * @param keyTwo
     * @param keyThree
     * @param value1
     * @param value2
     */
    public static void addStat(final String keyOne, final String keyTwo, final String keyThree, final long value1,
            final long value2) {
        final Keys keys = new Keys(keyOne, keyTwo, keyThree);
        addStat(keys, value1, value2);
    }


    /**
     * @deprecated
     **/
    @Deprecated
    public static void addStatWithAppName(final String appName, final String keyOne, final String keyTwo,
            final String keyThree, final long value1, final long value2) {
        final Keys keys = new Keys(appName, keyOne, keyTwo, keyThree);
        addStat(keys, value1, value2);
    }


    public static void addMiddlewareStat(final String middlewareName, final String keyOne, final String keyTwo,
            final String keyThree) {
        final Keys keys = new Keys(middlewareName, keyOne, keyTwo, keyThree);
        addMiddlewareStat(keys, 1, 0);
    }


    public static void addMiddlewareStat(final String middlewareName, final String keyOne, final String keyTwo,
            final String keyThree, final long value1, final long value2) {
        final Keys keys = new Keys(middlewareName, keyOne, keyTwo, keyThree);
        addMiddlewareStat(keys, value1, value2);
    }


    public static void addStat(final Keys keys, final long value1, final long value2) {
        setAppName(keys.getAppName());
        final ValueObject v = getAppValueObject(keys);
        if (v != null) {
            v.addCount(value1, value2);
        }
    }


    public static void immediateAddStat(final String keyOne, final String keyTwo, final String keyThree,
            final long value1, final long value2) {
        final StringBuilder sb = new StringBuilder();
        final SimpleDateFormat dateFormat = new SimpleDateFormat(MonitorConstants.DATE_FORMAT);
        final String appTime = dateFormat.format(Calendar.getInstance().getTime());

        sb.append(keyOne).append(MonitorConstants.SPLITTER_1);
        sb.append(keyTwo).append(MonitorConstants.SPLITTER_1);
        sb.append(keyThree).append(MonitorConstants.SPLITTER_1);
        sb.append(value1).append(MonitorConstants.SPLITTER_1);
        sb.append(value2).append(MonitorConstants.SPLITTER_1);
        sb.append(appTime).append(MonitorConstants.SPLITTER_1);
        sb.append(hostName).append(MonitorConstants.SPLITTER_2);
        appStatLog.info(sb);
    }


    private static void addMiddlewareStat(final Keys keys, final long value1, final long value2) {
        final ValueObject v = getMiddlewareValueObject(keys);
        if (v != null) {
            v.addCount(value1, value2);
        }
    }


    public static boolean isWriteLog() {
        return writeLog;
    }


    public static void setWriteLog(final boolean writeLog) {
        MonitorLog.writeLog = writeLog;
    }


    public long getWaitTime() {
        return waitTime;
    }


    public void setWaitTime(final long waitTime) {
        MonitorLog.waitTime = waitTime;
        MonitorLog.runWriteThread();
    }


    public boolean getJVMInfoPower() {
        return jvmInfoPower;
    }


    public void setJVMInfoPower(final boolean power) {
        MonitorLog.jvmInfoPower = power;
    }


    private static void setAppName(final String name) {
        if (name != null && appName == null) {
            appName = name;
        }
    }


    public int getAppMaxKey() {
        return appMaxKey;
    }


    public void setAppMaxKey(final int appMaxKey) {
        MonitorLog.appMaxKey = appMaxKey;
    }


    public int getMiddlewareMaxKey() {
        return middlewareMaxKey;
    }


    public void setMiddlewareMaxKey(final int middlewareMaxKey) {
        MonitorLog.middlewareMaxKey = middlewareMaxKey;
    }


    /**
     * 这个方法将确保不会返回null。（当keys无对应的ValueObject时创建之）
     */
    protected static ValueObject getAppValueObject(final Keys keys) {
        ValueObject v = appDatas.get(keys);
        if (null == v) {
            lock.lock();
            try {
                v = appDatas.get(keys);
                if (null == v) {
                    if (appDatas.size() <= appMaxKey) {
                        v = new ValueObject();
                        appDatas.put(keys, v);
                    }
                    else {
                        logger.warn("sorry,monitor app key is out size of " + appMaxKey);
                    }
                }
            }
            finally {
                lock.unlock();
            }
        }
        return v;
    }


    /**
     * 这个方法将确保不会返回null。（当keys无对应的ValueObject时创建之）
     */
    protected static ValueObject getMiddlewareValueObject(final Keys keys) {
        ValueObject v = middlewareDatas.get(keys);
        if (null == v) {
            middlewareLock.lock();
            try {
                v = middlewareDatas.get(keys);
                if (null == v) {
                    if (middlewareDatas.size() <= middlewareMaxKey) {
                        v = new ValueObject();
                        middlewareDatas.put(keys, v);
                    }
                    else {
                        logger.warn("sorry,monitor middleware key is out size of " + middlewareMaxKey);
                    }
                }
            }
            finally {
                middlewareLock.unlock();
            }
        }
        return v;
    }


    /**
     * <p>
     * 把TBCacheManager自己注册到MBeanServer
     * </p>
     * 
     */
    public static synchronized void registerSelf() {
        // MonitorLog statLog = new MonitorLog();
        // try {
        // ObjectName objectName = new
        // ObjectName(MonitorConstants.LOG_STAT_NAME_STAT_LOG);
        // if (!mbs.isRegistered(objectName)) {
        // mbs.registerMBean(statLog, objectName);
        // }
        // } catch (MBeanException e) {
        // logger.error(e.getMessage(), e);
        // } catch (MalformedObjectNameException e) {
        // logger.error(e.getMessage(), e);
        // } catch (NotCompliantMBeanException e) {
        // logger.error(e.getMessage(), e);
        // } catch (InstanceAlreadyExistsException e) {
        // logger.error(e.getMessage(), e);
        // }
    }


    /**
     * 监控jvm信息
     **/
    // private static String getJvmInfo(String time) {
    // DecimalFormat decimalFormat = new DecimalFormat("#.##");
    // MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    //
    // StringBuilder sb = new StringBuilder();
    // // jvm 内存信息 TC JVM MEMORY SITUATION 1024 856 2009-04-01 18:06:56 vm4.qa
    // if(appName != null){
    // sb.append(appName).append(MonitorConstants.SPLITTER_1);
    // }
    // sb.append(MonitorConstants.KEY_LEVEL_JVM).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_MEMORY).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_SITUATION).append(MonitorConstants.SPLITTER_1);
    // Double maxValue = new Double(memoryUsage.getMax()) / 1024 / 1024; //
    // 换算成单位 M
    // Double usedValue = new Double(memoryUsage.getUsed()) / 1024 / 1024;
    // sb.append(decimalFormat.format(maxValue)).append(MonitorConstants.SPLITTER_1);
    // sb.append(decimalFormat.format(usedValue)).append(MonitorConstants.SPLITTER_1);
    // sb.append(time).append(MonitorConstants.SPLITTER_1);
    // sb.append(hostName).append(MonitorConstants.SPLITTER_2);
    //
    // // jvm CPU信息 TC JVM CPU USAGE 68 0 2009-04-01 18:06:56 vm4.qa
    // if(appName != null){
    // sb.append(appName).append(MonitorConstants.SPLITTER_1);
    // }
    // sb.append(MonitorConstants.KEY_LEVEL_JVM).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_CPU).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_USAGE).append(MonitorConstants.SPLITTER_1);
    // Double threadCupTime = new Double(threadMXBean.getCurrentThreadCpuTime())
    // / 1000000000; // 换算成单位 :秒
    // sb.append(decimalFormat.format(threadCupTime)).append(MonitorConstants.SPLITTER_1);
    // sb.append("0").append(MonitorConstants.SPLITTER_1);
    // sb.append(time).append(MonitorConstants.SPLITTER_1);
    // sb.append(hostName).append(MonitorConstants.SPLITTER_2);
    //
    // // jvm 线程信息 TC JVM THREAD TOTAL 100 0 2009-04-01 18:06:56 vm4.qa
    // if(appName!=null){
    // sb.append(appName).append(MonitorConstants.SPLITTER_1);
    // }
    // sb.append(MonitorConstants.KEY_LEVEL_JVM).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_THREAD).append(MonitorConstants.SPLITTER_1);
    // sb.append(MonitorConstants.KEY_LEVEL_TOTAL).append(MonitorConstants.SPLITTER_1);
    // sb.append(threadMXBean.getDaemonThreadCount()).append(MonitorConstants.SPLITTER_1);
    // sb.append("0").append(MonitorConstants.SPLITTER_1);
    // sb.append(time).append(MonitorConstants.SPLITTER_1);
    // sb.append(hostName).append(MonitorConstants.SPLITTER_2);
    // return sb.toString();
    // }

    /**
     * 读取配置信息
     * */
    // public static long parseConfig(String filename){
    // long time = 0L;
    // DocumentBuilderFactory domfac=DocumentBuilderFactory.newInstance();
    // InputStream is = null;
    // try {
    // DocumentBuilder dombuilder=domfac.newDocumentBuilder();
    // is=new FileInputStream(filename);
    // Document doc=dombuilder.parse(is);
    // Element root=doc.getDocumentElement();
    // NodeList nodelist = root.getElementsByTagName("cache-time");
    // Node node = nodelist.item(0);
    // String timeStr = node.getTextContent();
    // time = Long.parseLong(timeStr);
    // } catch (Exception e){
    // return waitTime;
    // } finally {
    // if(is!=null){
    // try {
    // is.close();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    // return waitTime;
    // }

    public static void main(final String args[]) {
        // System.out.println(MonitorLog.parseConfig("c://test.xml"));
        final Thread testwriteThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    timerLock.lock();
                    try {
                        condition.await(5, TimeUnit.MILLISECONDS);
                    }
                    catch (final Exception e) {
                        logger.error("wait error", e);
                    }
                    finally {
                        timerLock.unlock();
                    }
                    MonitorLog.addStat("TC2", null, null, 541, 256);
                    MonitorLog.addStat("TC1", "SyncCreateOrder", "ERROR", 541, 256); // ok
                    MonitorLog.addStat("TC3", "SyncCreateOrder", null, 541, 256);
                    MonitorLog.addStat("UIC4", "SyncCreateOrder", "timout", 5621, 10);
                    MonitorLog.addStat("IC5", "SyncCreateOrder", "normal", 999, 5221);
                    MonitorLog.addStatWithAppName("appName", "IC6", "SyncCreateOrder", "normal", 999, 5221);
                    MonitorLog.addStatWithAppName("appName1", null, null, null, 999, 5221);// ok
                    MonitorLog.addStatWithAppName("appName2", "SyncCreateOrder", null, null, 999, 5221);
                    MonitorLog.addStatWithAppName("appName3", "SyncCreateOrder", null, null, 999, 5221);
                }
            }
        });
        testwriteThread.setName("TestTreadName");
        testwriteThread.start();
    }

}