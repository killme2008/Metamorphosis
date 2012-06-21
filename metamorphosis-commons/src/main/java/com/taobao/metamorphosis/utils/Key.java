package com.taobao.metamorphosis.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * A valid config key.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Key {
    public String name() default "";
}
