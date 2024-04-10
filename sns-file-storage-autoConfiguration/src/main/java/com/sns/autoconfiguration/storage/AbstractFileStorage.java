package com.sns.autoconfiguration.storage;

import com.alibaba.fastjson.JSON;
import com.sns.autoconfiguration.entity.PositionInfo;
import com.sns.autoconfiguration.entity.StorageEntity;
import com.sns.autoconfiguration.utils.Constants;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/4/28 16:52
 */
@Slf4j
public abstract class AbstractFileStorage<T> {


    /**
     * 索引map  全局唯一
     * key：filePath
     * value:  key:关键字索引  value:位置长度信息
     */
    public final Map<String, HashMap<String, Set<PositionInfo>>> indexMap = new ConcurrentHashMap<>();

    /**
     * 存储还未合并到文件的数据  全局唯一
     * key：key 例如：userId
     * value:  数据集合
     */
    public final Map<String, Set<StorageEntity<T>>> currentData = new ConcurrentHashMap<>();

    public static String BASE_PATH;
    public static int FILE_COUNT;

    public AbstractFileStorage() {
        BASE_PATH = "/usr/local/file_db";
        FILE_COUNT = 16;
    }

    public AbstractFileStorage(String basePath, int fileCount) {
        BASE_PATH = StringUtils.isBlank(basePath) ? "/usr/local/file_db" : basePath;
        FILE_COUNT = fileCount > 0 ? fileCount : 16;
    }

    public abstract void init();

    /**
     * 计算要写入数据应该初始化大小
     * @param dataSet
     * @return
     */
    protected long computeDataInitSize(Set<StorageEntity<T>> dataSet) {
        if (CollectionUtils.isEmpty(dataSet)) {
            return 0;
        }
        int size = 0;
        for (StorageEntity<T> data : dataSet) {
            size += data.getKey().getBytes(StandardCharsets.UTF_8).length;
            size += JSON.toJSONString(data.getDataSet()).getBytes(StandardCharsets.UTF_8).length;
            size += String.valueOf(data.getStatus()).getBytes(StandardCharsets.UTF_8).length;
        }
        return Integer.valueOf(size).longValue();
    }

    /**
     * 计算要写入索引应该初始化大小
     * @param indexMap
     * @return
     */
    protected long computeIndexInitSize(Map<String, Set<PositionInfo>> indexMap) {
        if (MapUtils.isEmpty(indexMap)) {
            return 0;
        }
        int size = 0;
        for (Entry<String, Set<PositionInfo>> entry : indexMap.entrySet()) {
            // key的长度 int型
            size += Integer.BYTES;
            // key占用空间
            size += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            // position的个数
            size += Integer.BYTES;
            // position占用空间
            size += Integer.BYTES * 3 * entry.getValue().size();
        }
        return Integer.valueOf(size).longValue();
    }

    /**
     * 获取
     * @param byteBuffer
     * @return
     */
    protected String getBufferString(ByteBuffer byteBuffer, int length) {
        try {
            byte[] bytes = new byte[length];
            byteBuffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int getFileBucket(String key) {
        return Hashing.consistentHash(key.hashCode(), FILE_COUNT);
    }


    public static String getDataFilePath(int index) {
        return StringUtils.stripEnd(BASE_PATH, "/") + "/" + Constants.FILE_PREFIX + index + Constants.DATA_SUFFIX;
    }

    public static String getDataFilePath(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        int index = getFileBucket(key);
        return StringUtils.stripEnd(BASE_PATH, "/") + "/" + Constants.FILE_PREFIX + index + Constants.DATA_SUFFIX;
    }

    public static String getFilePathByFileName(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return StringUtils.stripEnd(BASE_PATH, "/") + "/error.data";
        }
        return StringUtils.stripEnd(BASE_PATH, "/") + "/" + fileName;
    }

    public static String indexNameToDataName(String indexFileName) {
        if (StringUtils.isBlank(indexFileName)) {
            return null;
        }
        return indexFileName.replace(Constants.INDEX_SUFFIX, Constants.DATA_SUFFIX);
    }

    public static String getIndexFilePath(int index) {
        return StringUtils.stripEnd(BASE_PATH, "/") + "/" + Constants.FILE_PREFIX + index + Constants.INDEX_SUFFIX;
    }

    /**
     * 根据数据文件路径 查找 索引文件路径
     * @param filePath
     * @return
     */
    public static String getIndexFilePath(String filePath) {
        String fileIndex = StringUtils.substringBetween(filePath, Constants.FILE_PREFIX, Constants.DATA_SUFFIX);
        if (StringUtils.isBlank(fileIndex)) {
            return null;
        }
        return StringUtils.stripEnd(BASE_PATH, "/") + "/" + Constants.FILE_PREFIX + fileIndex + Constants.INDEX_SUFFIX;
    }
}
