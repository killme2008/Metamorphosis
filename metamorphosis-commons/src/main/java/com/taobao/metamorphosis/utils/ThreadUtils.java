package com.taobao.metamorphosis.utils;

/**
 * Created with IntelliJ IDEA.
 * User: dennis (xzhuang@avos.com)
 * Date: 13-3-3
 * Time: ионГ11:26
 */
public class ThreadUtils {
    public static RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new IllegalStateException("Not unchecked", t);
        }
    }
}
