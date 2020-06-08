package com.hailin.job.schedule.executor;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;

/**
 * 调度的类加载
 * 先自己尝试加载， 否则采用双亲委派机制进行加载
 */

public class ScheduleClassLoader extends URLClassLoader {

    public ScheduleClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)){
            Class<?> findClass = findLoadedClass(name);
            if (Objects.isNull(findClass)){
                findClass = super.loadClass(name , resolve);
            }
            return findClass;
        }
    }
}
