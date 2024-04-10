package com.sns.autoconfiguration.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/7/26 16:37
 */
@ConfigurationProperties(prefix = "sns.file.storage")
public class StorageConfig {
    /** 文件数量 */
    private int fileCount = 16;
    /** 文件路径 */
    private String basePath = "/usr/local/file_db";
    /** 扩容因子 */
    private float resizeFactor = 0.8f;
    /** 整理文件任务执行时间设置   间隔几天_时_分_秒 */
    private String fixArrangeTime = "1_3_0_0";

    public int getFileCount() {
        return fileCount <= 0 ? 16 : fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public String getBasePath() {
        return StringUtils.isBlank(basePath) ? "/usr/local/file_db" : basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public float getResizeFactor() {
        return resizeFactor < 0.5f || resizeFactor > 1f ? 0.8f : resizeFactor;
    }

    public void setResizeFactor(float resizeFactor) {
        this.resizeFactor = resizeFactor;
    }

    public String getFixArrangeTime() {
        String addDays = "1";
        String hour = "3";
        String minute = "0";
        String second = "0";
        String[] times = fixArrangeTime.split("_");
        if (times.length >= 4) {
            if (StringUtils.isNumeric(times[0]) && Integer.parseInt(times[0]) > 0 & Integer.parseInt(times[0]) <= 365) {
                addDays = times[0];
            }
            if (StringUtils.isNumeric(times[1]) && Integer.parseInt(times[1]) >= 0 && Integer.parseInt(times[1]) < 24) {
                hour = times[1];
            }
            if (StringUtils.isNumeric(times[2]) && Integer.parseInt(times[2]) >= 0 && Integer.parseInt(times[1]) < 60) {
                minute = times[2];
            }
            if (StringUtils.isNumeric(times[3]) && Integer.parseInt(times[3]) >= 0 && Integer.parseInt(times[3]) < 60) {
                second = times[3];
            }
        }
        return addDays + "_" + hour + "_" + minute + "_" + second;
    }

    public void setFixArrangeTime(String fixArrangeTime) {
        this.fixArrangeTime = fixArrangeTime;
    }
}
