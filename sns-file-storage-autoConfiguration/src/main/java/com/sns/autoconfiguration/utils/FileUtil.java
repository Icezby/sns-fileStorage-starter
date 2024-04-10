package com.sns.autoconfiguration.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public class FileUtil {

    public static final ExecutorService executor = new ThreadPoolExecutor(
        20,
        50,
        1L,
        TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(500),
        new ThreadFactoryBuilder().build(), new CallerRunsPolicy());

    public static final ExecutorService writeFileExecutor = new ThreadPoolExecutor(
        10,
        10,
        1L,
        TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(500),
        new ThreadFactoryBuilder().build(), new CallerRunsPolicy());

    /**
     * 整理文件线程池
     */
    public static final ExecutorService arrangeExecutor = new ThreadPoolExecutor(
        5,
        10,
        1L,
        TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(100),
        new ThreadFactoryBuilder().build(), new CallerRunsPolicy());


    public static  boolean exist(String filePath) {
        if (null == filePath) {
            return false;
        }
        File file = new File(filePath);
        return file.exists();
    }

    public static boolean deleteFile(String filePath) {
        if (StringUtils.isBlank(filePath)){
            return false;
        }
        File file = new File(filePath);
        return file.delete();
    }

}
