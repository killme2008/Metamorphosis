package com.taobao.metamorphosis.utils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;


public abstract class Config {
    public Set<String> getFieldSet() {
        Class<? extends Config> clazz = this.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> rt = new HashSet<String>();
        for (Field f : fields) {
            String name = f.getName();
            Ignore ignore = f.getAnnotation(Ignore.class);
            if (ignore != null) {
                continue;
            }
            Key key = f.getAnnotation(Key.class);
            if (key != null) {
                name = key.name();
                if (!StringUtils.isBlank(name)) {
                    rt.add(name);
                }
                else {
                    rt.add(f.getName());
                }
            }
            else if (name.length() > 0 && Character.isLowerCase(name.charAt(0))) {
                rt.add(name);
            }
        }
        return rt;
    }


    public String findBestMatchField(Set<String> fields, String value) {
        int minScore = Integer.MAX_VALUE;
        String matchedField = null;
        for (String f : fields) {
            int dis = StringUtils.getLevenshteinDistance(value, f);
            if (dis < minScore) {
                matchedField = f;
                minScore = dis;
            }
        }
        return matchedField;
    }


    public void checkConfigKeys(Set<String> configKeySet, Set<String> validKeySet) {
        for (String key : configKeySet) {
            if (!validKeySet.contains(key)) {
                String best = this.findBestMatchField(validKeySet, key);
                throw new IllegalArgumentException("Invalid config key:" + key + ",do you mean '" + best + "'?");
            }
        }
    }
}
