package com.sns.autoconfiguration.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sns.autoconfiguration.config.StorageConfig;
import com.sns.autoconfiguration.entity.PositionInfo;
import com.sns.autoconfiguration.entity.StorageEntity;
import com.sns.autoconfiguration.utils.Constants;
import com.sns.autoconfiguration.utils.FileUtil;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/4/10 10:14
 */
@Slf4j
public class DefaultFileStorage<T> extends AbstractFileStorage<T> {

    private static StorageConfig storageConfig;

    public DefaultFileStorage(){
        super();
    }

    public DefaultFileStorage(StorageConfig config) {
        super(config.getBasePath(), config.getFileCount());
        storageConfig = config;
    }


    @Override
    @PostConstruct
    public void init() {
        initIndex();
        // 整理文件任务
        runArrangeTask();
    }

    /**
     * 读取索引文件，加载内存
     */
    private void initIndex() {
        long start = System.currentTimeMillis();
        log.info("initIndex start:{}", start);
        File file = new File(BASE_PATH);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            if (file.exists() & file.isDirectory() && ArrayUtils.isNotEmpty(file.listFiles())) {
                for (File listFile : file.listFiles()) {
                    if (!listFile.getName().startsWith(Constants.FILE_PREFIX) || !listFile.getName().endsWith(Constants.INDEX_SUFFIX)) {
                        continue;
                    }
                    String indexFilePath = getFilePathByFileName(listFile.getName());
                    MmapFile mmapFile = MmapFile.getOrLoad(indexFilePath);
                    if (Objects.isNull(mmapFile)) {
                        log.info("initIndex mmap is null, filePath:{}", indexFilePath);
                        continue;
                    }
                    HashMap<String, Set<PositionInfo>> indexHashMap = new HashMap<>();
                    ByteBuffer byteBuffer = mmapFile.getByteBuffer();
                    log.info("initIndex byteBuffer , position:{}, filePath:{}", byteBuffer.position(), indexFilePath);
                    while (byteBuffer.hasRemaining()) {
                        int keyLength = byteBuffer.getInt();
                        if (keyLength <= 0) break;
                        log.info("keyLength：{}", keyLength);
                        byte[] bytes = new byte[keyLength];
                        byteBuffer.get(bytes);
                        String key = new String(bytes, StandardCharsets.UTF_8);
                        int count = byteBuffer.getInt();
                        log.info("look initIndex key:{}, count:{}, filePath:{}, byteBuffer:{}", key, count, indexFilePath, byteBuffer);
                        Set<PositionInfo> positionInfos = indexHashMap.get(key);
                        for (int i = 0; i < count; i++) {
                            int position = byteBuffer.getInt();
                            int keyL = byteBuffer.getInt();
                            int valL = byteBuffer.getInt();
                            if (CollectionUtils.isEmpty(positionInfos)) {
                                Set<PositionInfo> positionList = new HashSet<>();
                                positionList.add(new PositionInfo(position, keyL, valL));
                                indexHashMap.put(key, positionList);
                            }else {
                                positionInfos.add(new PositionInfo(position, keyL, valL));
                            }
                        }
                    }
                    log.info("initIndex file:{}, indexMap:{}", indexFilePath, JSON.toJSONString(indexHashMap));
                    indexMap.put(getFilePathByFileName(indexNameToDataName(listFile.getName())), indexHashMap);
                }
            }
        }catch (Exception e) {
            log.error("init index error:", e);
        }
        log.info("init index end!, cost:{}, indexMap:{}", System.currentTimeMillis() - start, JSON.toJSONString(indexMap));

    }


    /**
     * 写入文件
     * @param dataSet
     * @param path  文件路径，如果为空的
     * @throws Exception
     */
    public boolean writeToFile(Set<StorageEntity<T>> dataSet, String path) {
        if (CollectionUtils.isEmpty(dataSet)) {
            return false;
        }
        Set<StorageEntity<T>> storageEntitySet = mergeSameParams(dataSet);
        log.info("writeToFile after merge:{}", JSON.toJSONString(storageEntitySet));
        Map<String, Set<StorageEntity<T>>> dataMap = new HashMap<>();
        if (StringUtils.isBlank(path))  {
            for (StorageEntity<T> storageEntity : storageEntitySet) {
                String filePath = getDataFilePath(storageEntity.getKey());
                if (StringUtils.isBlank(filePath)) continue;
                dataMap.computeIfAbsent(filePath, key -> new HashSet<>()).add(storageEntity);
            }
        }else {
            dataMap.put(path, dataSet);
        }
        // 写入文件
        List<Future<Boolean>> futureList = new ArrayList<>();
        for (Entry<String, Set<StorageEntity<T>>> entry : dataMap.entrySet()) {
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(
                () -> doWriteToFile(entry.getValue(), entry.getKey()), FileUtil.writeFileExecutor);
            futureList.add(future);
        }
        for (Future<Boolean> future : futureList) {
            try {
                if (!future.get()) {
                    log.error("multi doWriteToFile failed");
                    return false;
                }
            }catch (Exception e) {
                log.error("multi doWriteToFile error:", e);
            }
        }
        return true;
    }

    /**
     * 写入文件
     * @param storageEntitySet
     * @param filePath
     * @return
     */
    private boolean doWriteToFile(Set<StorageEntity<T>> storageEntitySet, String filePath) {
        long start = System.currentTimeMillis();
        log.info("doWriteToFile start:{}, filePath:{}", start, filePath);
        if (CollectionUtils.isEmpty(storageEntitySet) || StringUtils.isBlank(filePath)) {
            return false;
        }
        //计算数据大小
        long initSize = computeDataInitSize(storageEntitySet);
        if(initSize <= 0L) {
            log.error("initSize is 0 StorageDTO:{}", JSON.toJSONString(storageEntitySet));
            return false;
        }
        try {
            // 每个文件写入时需要加锁
            synchronized (filePath.intern()) {
                log.info("########Thread：{}拿到锁了, filePath:{}", Thread.currentThread().getName(), filePath);
                //获取mmapFile
                MmapFile mmapFile = MmapFile.getOrCreate(filePath, initSize, storageConfig.getResizeFactor());
                if (Objects.isNull(mmapFile)) {
                    return false;
                }
                ByteBuffer byteBuffer = mmapFile.getByteBuffer();
                int position = mmapFile.getWritePosition();
                byteBuffer.position(position);
                log.info("debug~~~~~~~~~~~~position:{}, mmapFile:{}, byteBuffer:{}", position, mmapFile, byteBuffer);
                // 获取索引信息
                HashMap<String, Set<PositionInfo>> indexInfoMap = new HashMap<>();
                for (StorageEntity<T> storageEntity : storageEntitySet) {
                    PositionInfo positionInfo = new PositionInfo();
                    positionInfo.setPosition(position);
                    byte[] keyBytes = storageEntity.getKey().getBytes(StandardCharsets.UTF_8);
                    byteBuffer.put(keyBytes);
                    positionInfo.setKeyLength(keyBytes.length);

                    byte[] valueBytes = JSON.toJSONString(storageEntity.getDataSet()).getBytes(StandardCharsets.UTF_8);
                    byteBuffer.put(valueBytes);
                    positionInfo.setValueLength(valueBytes.length);

                    indexInfoMap.computeIfAbsent(storageEntity.getKey(), key -> new HashSet<>()).add(positionInfo);

                    byte[] statusBytes = String.valueOf(storageEntity.getStatus()).getBytes(StandardCharsets.UTF_8);
                    byteBuffer.put(statusBytes);

                    position += keyBytes.length + valueBytes.length + statusBytes.length;
                }
                mmapFile.setWritePosition(position);
                mmapFile.getByteBuffer().putInt(0, position);
                log.info("debug####写完文件后的writePosition：{}", mmapFile.getWritePosition());
                if (MapUtils.isNotEmpty(indexInfoMap)) {
                    // 索引写内存
                    writeIndexToCache(indexInfoMap, filePath);
                    // 索引写文件
                    writeIndexToFile(indexInfoMap, filePath);
                }
            }
            log.info("doWriteToFile end, filePath:{}, cost:{}", filePath, System.currentTimeMillis() - start);
        }catch (Exception e) {
            log.error("writeToFile error, path:{}, e:", filePath, e);
            return false;
        }
        return true;
    }

    /**
     * 索引写入内存
     * @param indexInfoMap
     * @param filePath
     */
    private void writeIndexToCache(HashMap<String, Set<PositionInfo>> indexInfoMap, String filePath) {
        if (MapUtils.isEmpty(indexInfoMap) || StringUtils.isBlank(filePath)) {
            return;
        }
        HashMap<String, Set<PositionInfo>> nowIndexMap = indexMap.get(filePath);
        if (null == nowIndexMap) {
           indexMap.put(filePath, new HashMap<>());
           nowIndexMap = indexMap.get(filePath);
        }
        for (Entry<String, Set<PositionInfo>> entry : indexInfoMap.entrySet()) {
            nowIndexMap.computeIfAbsent(entry.getKey(), key -> new HashSet<>()).addAll(entry.getValue());
        }
        log.info("debug##### filePath:{}写完后的indexMap:{}", filePath, JSON.toJSONString(indexMap.get(filePath)));
    }

    /**
     * 追加索引到文件中
     * @param indexInfoMap
     * @param filePath
     * @return
     */
    public boolean writeIndexToFile(Map<String, Set<PositionInfo>> indexInfoMap, String filePath) {
        if (MapUtils.isEmpty(indexInfoMap) || StringUtils.isBlank(filePath)) {
            return false;
        }
        long start = System.currentTimeMillis();
        String indexFilePath = getIndexFilePath(filePath);
        if (StringUtils.isBlank(indexFilePath)) {
            log.error("writeIndexToFile indexFilePath is null, filePath:{}", filePath);
            return false;
        }
        long initSize = computeIndexInitSize(indexInfoMap);
        if (initSize <= 0) {
            log.error("writeIndexToFile initSize is 0, indexInfoMap:{}", JSON.toJSONString(indexInfoMap));
            return false;
        }
        MmapFile mmapFile = MmapFile.getOrCreate(indexFilePath, initSize, storageConfig.getResizeFactor());
        if (Objects.isNull(mmapFile)) {
            return false;
        }
        try{
            ByteBuffer byteBuffer = mmapFile.getByteBuffer();
            byteBuffer.position(mmapFile.getWritePosition());
            log.info("writeIndexToFile mmap:{}, wPosition:{} byteBuffer :{}, indexFilePath:{}",mmapFile, mmapFile.getWritePosition(), byteBuffer, indexFilePath);
            for (Entry<String, Set<PositionInfo>> entry : indexInfoMap.entrySet()) {
                byte[] bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
                byteBuffer.putInt(entry.getValue().size());
                log.info("writeIndexToFile position, filePath:{}, positions:{}", filePath, JSON.toJSONString(entry.getValue()));
                for (PositionInfo positionInfo : entry.getValue()) {
                    byteBuffer.putInt(positionInfo.getPosition());
                    byteBuffer.putInt(positionInfo.getKeyLength());
                    byteBuffer.putInt(positionInfo.getValueLength());
                }
            }
            mmapFile.setWritePosition(byteBuffer.position());
            mmapFile.getByteBuffer().putInt(0, byteBuffer.position());
            log.info("debug### index 写完后byteBuffer:{}", byteBuffer);
        }catch (Exception e) {
            log.error("writeIndexToFile error, filePath:{},indexInfoMap:{} ", filePath, JSON.toJSONString(indexInfoMap));
            return false;
        }
        log.info("writeIndexToFile end, filePath:{}, cost:{}", filePath, System.currentTimeMillis() - start);
        return true;
    }

    /**
     * 对入参进行合并，保证相同key只存在一个。
     * @param storageEntities
     * @return
     */
    private Set<StorageEntity<T>> mergeSameParams(Set<StorageEntity<T>> storageEntities) {
        if (CollectionUtils.isEmpty(storageEntities)) {
            return storageEntities;
        }
        Map<String, List<StorageEntity<T>>> dataMap = storageEntities.stream()
            .filter(x -> Objects.nonNull(x) && x.getStatus() == '0')
            .collect(Collectors.groupingBy(StorageEntity::getKey));
        if (MapUtils.isEmpty(dataMap)) {
            return storageEntities;
        }
        Set<StorageEntity<T>> storageEntitySet = new HashSet<>();
        for (Entry<String, List<StorageEntity<T>>> entry : dataMap.entrySet()) {
            Set<T> valueSet = new HashSet<>();
            entry.getValue().forEach(x -> valueSet.addAll(x.getDataSet()));
            storageEntitySet.add(new StorageEntity<>(entry.getKey(), valueSet, '0'));
        }
        return storageEntitySet;
    }

    /**
     * 读取文件全部内容
     * @param filePath
     * @return
     */
    public Set<StorageEntity<T>> readFileAllSet(String filePath) {
        if (StringUtils.isBlank(filePath)) return new HashSet<>();
        MmapFile mmapFile = MmapFile.getOrLoad(filePath);
        log.info("readFileAllSet, filePath:{}, mmap:{}", filePath, mmapFile);
        if (Objects.isNull(mmapFile)) {
            return new HashSet<>();
        }
        Set<StorageEntity<T>> result = new HashSet<>();
        ByteBuffer byteBuffer = mmapFile.getByteBuffer();
        HashMap<String, Set<PositionInfo>> indexPositionMap = indexMap.get(filePath);
        if (MapUtils.isEmpty(indexPositionMap)) {
            return new HashSet<>();
        }
        for (Set<PositionInfo> positionInfos : indexPositionMap.values()) {
            for (PositionInfo positionInfo : positionInfos) {
                StorageEntity<T> storageEntity = new StorageEntity<>();
                byteBuffer.position(positionInfo.getPosition());
                storageEntity.setKey(getBufferString(byteBuffer, positionInfo.getKeyLength()));
                storageEntity.setDataSet(new HashSet<>(JSON.parseObject(getBufferString(byteBuffer, positionInfo.getValueLength()), new TypeReference<Set<T>>(){})));
                storageEntity.setStatus(getBufferString(byteBuffer, 1).charAt(0));
                result.add(storageEntity);
            }
        }
        return result;
    }

    public List<StorageEntity<T>> readFileAllList(String filePath) {
        if (StringUtils.isBlank(filePath)) return new ArrayList<>();
        MmapFile mmapFile = MmapFile.getOrLoad(filePath);
        if (Objects.isNull(mmapFile)) {
            return new ArrayList<>();
        }
        List<StorageEntity<T>> result = new ArrayList<>();
        ByteBuffer byteBuffer = mmapFile.getByteBuffer();
        HashMap<String, Set<PositionInfo>> indexPositionMap = indexMap.get(filePath);
        for (Set<PositionInfo> positionInfos : indexPositionMap.values()) {
            for (PositionInfo positionInfo : positionInfos) {
                StorageEntity<T> storageEntity = new StorageEntity<>();
                byteBuffer.position(positionInfo.getPosition());
                storageEntity.setKey(getBufferString(byteBuffer, positionInfo.getKeyLength()));
                storageEntity.setDataSet(new HashSet<>(JSON.parseObject(getBufferString(byteBuffer, positionInfo.getValueLength()), new TypeReference<Set<T>>(){})));
                storageEntity.setStatus(getBufferString(byteBuffer, 1).charAt(0));
                result.add(storageEntity);
            }
        }
        return result;
    }

    /**
     * 根据关键字查找内容
     * @param key
     * @return
     */
    public StorageEntity<T> getData(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        long start = System.currentTimeMillis();
        log.info("find by key start:{}, key:{}", start, key);
        StorageEntity<T> storageEntity = new StorageEntity<>();
        storageEntity.setKey(key);
        storageEntity.setStatus('0');
        Set<T> valueList = new HashSet<>();
        storageEntity.setDataSet(valueList);
        Set<StorageEntity<T>> storageEntities = currentData.get(key);

        if (CollectionUtils.isNotEmpty(storageEntities)) {
            for (StorageEntity<T> info : storageEntities) {
                if (Objects.nonNull(info) && info.getStatus() == '0') {
                    valueList.addAll(info.getDataSet());
                }
            }
        }
        String filePath = getDataFilePath(key);
        Set<PositionInfo> positionInfos = getPositionInfo(filePath, key);
        if (CollectionUtils.isEmpty(positionInfos)) {
            log.warn("MmapService.findStorageDTO.getPositionInfo is null, key:{}", key);
            return storageEntity;
        }
        MmapFile mmapFile = MmapFile.getOrLoad(filePath);
        if (Objects.isNull(mmapFile)) {
            log.warn("MmapService.findStorageDTO.getOrLoad is null");
            return storageEntity;
        }
        try {
            ByteBuffer byteBuffer = mmapFile.getByteBuffer();
            for (PositionInfo positionInfo : positionInfos) {
                byteBuffer.position(positionInfo.getPosition() + positionInfo.getKeyLength() + positionInfo.getValueLength());
                char status = getBufferString(byteBuffer, 1).charAt(0);
                if (status == '0') {
                    byteBuffer.position(positionInfo.getPosition() + positionInfo.getKeyLength());
                    valueList.addAll(new HashSet<>(JSON.parseObject(getBufferString(byteBuffer, positionInfo.getValueLength()), new TypeReference<Set<T>>(){})));
                }
            }
            log.info("find by key end, result:{},cost:{}", JSON.toJSONString(storageEntity), System.currentTimeMillis() - start);
        }catch (Exception e) {
            log.error("findByKey error, key:{}, filePath:{}, e:", key, filePath, e);
        }
        return storageEntity;
    }

    /**
     * 删除数据 -更新状态
     * @param key
     */
    public void delData(String key) {
        long start = System.currentTimeMillis();
        String filePath = getDataFilePath(key);
        Set<PositionInfo> positionInfos = getPositionInfo(filePath, key);
        if (CollectionUtils.isEmpty(positionInfos)) {
            log.warn("MmapService.findStorageDTO.deleteData is null, key:{}, filePath:{}", key, filePath);
            return;
        }
        MmapFile mmapFile = MmapFile.getOrLoad(filePath);
        if (Objects.isNull(mmapFile)) {
            log.warn("MmapService.deleteData.deleteData is null");
            return;
        }
        ByteBuffer byteBuffer = mmapFile.getByteBuffer();
        for (PositionInfo positionInfo : positionInfos) {
            byteBuffer.position(positionInfo.getPosition() + positionInfo.getKeyLength() + positionInfo.getValueLength());
            byteBuffer.put("1".getBytes(StandardCharsets.UTF_8));
        }
        log.info("MmapService.deleteData.deleteData cost:{}", System.currentTimeMillis() - start);
    }

    /**
     * 新增数据先放到内存中，然后写入文件。 写入成功，再清掉内存数据。
     * @param storageEntities
     * @throws Exception
     */
    public boolean putData(Set<StorageEntity<T>> storageEntities) {
        if (CollectionUtils.isEmpty(storageEntities)) {
            return false;
        }
        Set<StorageEntity<T>> allDataSet = new HashSet<>();
        for (StorageEntity<T> storageEntity : storageEntities) {
            currentData.computeIfAbsent(storageEntity.getKey(), key -> new HashSet<>()).add(storageEntity);
            allDataSet.addAll(currentData.get(storageEntity.getKey()));
        }
        log.info("addData allDataSet:{}", JSON.toJSONString(allDataSet));
        // 异步写入文件
        CompletableFuture.runAsync(() -> {
            long start = System.currentTimeMillis();
            log.info("Async write file start:{}", start);
            boolean writeFileOk = writeToFile(allDataSet, null);
            // 写入完成删除内存数据
            if (!writeFileOk) {
               log.error("addData writeToFile error, addDataSet:{}", JSON.toJSONString(allDataSet));
               return;
            }
            for (StorageEntity<T> data : allDataSet) {
                Set<StorageEntity<T>> infoSet = currentData.get(data.getKey());
                if (CollectionUtils.isNotEmpty(infoSet)) {
                    infoSet.remove(data);
                }
            }
            log.info("Async write file end, cost:{}", System.currentTimeMillis() - start);
        }, FileUtil.executor);
        return true;
    }

    /**
     * 获取指定文件的索引map
     * @param filePath
     * @return
     */
    private Set<PositionInfo> getPositionInfo(String filePath, String key) {
        if (StringUtils.isBlank(filePath) || StringUtils.isBlank(key)) {
            return null;
        }
        HashMap<String, Set<PositionInfo>> positionMap = indexMap.get(filePath);
        if (MapUtils.isEmpty(positionMap)) {
            return null;
        }
        return positionMap.get(key);
    }

    /**
     * 启动删除过期树定时任务
     * 启动之后 24小时执行一次
     */
    public void runArrangeTask() {
        String[] arrangeTime = storageConfig.getFixArrangeTime().split("_");
        log.info("runArrangeTask arrangeTime :{}", Arrays.toString(arrangeTime));
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().build());
        executorService.scheduleAtFixedRate(this::arrangeFileData, (getTaskExecTimeMillis() - System.currentTimeMillis()) / 1000,
            TimeUnit.DAYS.toSeconds(Integer.parseInt(arrangeTime[0])), TimeUnit.SECONDS);
    }

    /**
     * 取明天凌晨执行时间的毫秒数
     * @return
     */
    private long getTaskExecTimeMillis() {
        String[] timeArr = storageConfig.getFixArrangeTime().split("_");
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, Integer.parseInt(timeArr[0]));
        calendar.set(Calendar.AM_PM, Calendar.AM);
        calendar.set(Calendar.HOUR, Integer.parseInt(timeArr[1]));
        calendar.set(Calendar.MINUTE, Integer.parseInt(timeArr[2]));
        calendar.set(Calendar.SECOND, Integer.parseInt(timeArr[3]));
        return calendar.getTime().getTime();
    }


    /**
     * 整理数据文件 合并或清理已删除状态数据
     */
    public void arrangeFileData() {
        //读取全部数据，重新写入文件
        long start = System.currentTimeMillis();
        log.info("arrangeFileData task start:{}", start);
        File baseFile = new File(BASE_PATH);
        try {
            if (baseFile.exists() & baseFile.isDirectory()) {
                File[] files = baseFile.listFiles();
                if (null != files && files.length > 0) {
                    CountDownLatch countDownLatch = new CountDownLatch(files.length);
                    for (File file : files) {
                        CompletableFuture.runAsync(() -> doArrangeFileData(file, countDownLatch), FileUtil.arrangeExecutor);
                    }
                    countDownLatch.await();
                }
            }
        }catch (Exception e) {
            log.error("arrangeFileData error:", e);
        }
        log.info("arrangeFileData finished, cost:{}", System.currentTimeMillis() - start);
    }

    /**
     * 对文件进行数据整理
     * @param file
     */
    private void doArrangeFileData(File file, CountDownLatch countDownLatch) {
        try{
            if (!file.getName().startsWith(Constants.FILE_PREFIX) || !file.getName().endsWith(Constants.DATA_SUFFIX)) {
                return;
            }
            String filePath = getFilePathByFileName(file.getName());
            if (StringUtils.isBlank(filePath)) {
                return;
            }
            MmapFile mmapFile = MmapFile.getOrLoad(filePath);
            if (Objects.isNull(mmapFile)) {
                log.warn("arrangeFileData.getOrLoadMmapFile is null, filePath:{}", filePath);
                return;
            }
            Set<StorageEntity<T>> storageEntitySet = readFileAllSet(filePath);
            if (CollectionUtils.isEmpty(storageEntitySet)) {
                log.warn("arrangeFileData readFileAll is empty, filePath:{}", filePath);
                return;
            }
            Map<String, List<StorageEntity<T>>> storageEntityMap = storageEntitySet.stream()
                .filter(x -> Objects.nonNull(x) && x.getStatus() == '0')
                .collect(Collectors.groupingBy(StorageEntity::getKey));
            if (MapUtils.isEmpty(storageEntityMap)) {
                log.warn("arrangeFileData StorageDTOMap is empty, filePath:{}", filePath);
                return;
            }
            Set<StorageEntity<T>> allDataList = new HashSet<>();
            for (Entry<String, List<StorageEntity<T>>> entry : storageEntityMap.entrySet()) {
                StorageEntity<T> storageEntity = new StorageEntity<>();
                storageEntity.setKey(entry.getKey());
                storageEntity.setStatus('0');
                Set<T> valueSet = new HashSet<>();
                for (StorageEntity<T> info : entry.getValue()) {
                    valueSet.addAll(info.getDataSet());
                }
                storageEntity.setDataSet(valueSet);
                allDataList.add(storageEntity);
            }
            log.info("arrangeFileData allDataList :{}", JSON.toJSONString(allDataList));
            String newFilePath = getFilePathByFileName(file.getName()) + ".temp";
            boolean toFileOk = writeToFile(allDataList, newFilePath);
            log.info("######debug, writeToFile result:{}, path:{}", toFileOk, newFilePath);
            if (!toFileOk) {
                log.error("arrangeFileData failed, path:{}", filePath);
                // 写入失败则删除新文件和索引
                indexMap.remove(newFilePath);
                File tempFile = new File(newFilePath);
                if (tempFile.exists()) {
                    tempFile.delete();
                }
                return;
            }
            synchronized (filePath.intern()) {
                File tempFile = new File(newFilePath);
                if (!file.exists() || !tempFile.exists()) {
                    log.error("oldFile or newFile not exists, filePath:{}", filePath);
                    return;
                }
                String oldFilePath = getFilePathByFileName(file.getName());
                // 先把原来文件换个名
                File bakFile = new File(oldFilePath + ".bak");
                MmapFile.unload(oldFilePath);
                boolean oldFileRename = file.renameTo(bakFile);
                if (!oldFileRename) {
                    log.error("oldFile rename failed, filePath:{}", oldFilePath);
                    return;
                }
                // 新文件改为原来文件
                boolean renameOk = tempFile.renameTo(file);
                if (!renameOk) {
                    // 尝试恢复原文件
                    boolean renameBak = bakFile.renameTo(file);
                    log.error("bakFile rename failed, tempFile:{}, nameTo:{}, recover oldFile result:{}", tempFile.getName(), oldFilePath, renameBak);
                    return;
                }
                MmapFile.mmapFileMap.put(oldFilePath, MmapFile.mmapFileMap.get(newFilePath));
                MmapFile.mmapFileMap.remove(newFilePath);
                HashMap<String, Set<PositionInfo>> newIndexMap = indexMap.get(newFilePath);
                if (MapUtils.isEmpty(newIndexMap)) {
                    log.error("del oldFile indexMap is empty, filePath:{}", filePath);
                    return;
                }
                indexMap.put(oldFilePath, newIndexMap);
                indexMap.remove(newFilePath);
                // 写入indexFile
                String indexFilePath = getIndexFilePath(oldFilePath);
                MmapFile.unload(indexFilePath);
                boolean deleteIndex = FileUtil.deleteFile(indexFilePath);
                if (deleteIndex){
                    boolean writeRes = writeIndexToFile(newIndexMap, oldFilePath);
                    log.info("doArrangeFileData write index file：{}, path:{}, mmap:{}", writeRes, oldFilePath, MmapFile.mmapFileMap.get(oldFilePath));
                }else {
                    log.error("delete index file failed, indexFilePath:{}, newIndexMap:{}", indexFilePath, JSON.toJSONString(newIndexMap));
                }
                // 最后删除 .bak文件
                boolean deleteRes = bakFile.delete();
                log.info("the end file is:{}, bakFile delete result:{}", file.getPath(), deleteRes);
            }
        } catch (Exception e) {
            log.info("doArrangeFileData error:", e);
        }finally {
            countDownLatch.countDown();
        }
    }

    public HashMap<String, List<PositionInfo>> readAllIndex(String indexFilePath) {
        MmapFile mmapFile = MmapFile.getOrLoad(indexFilePath);
        if (Objects.isNull(mmapFile)) {
            log.info("initIndex mmap is null, filePath:{}", indexFilePath);
            return new HashMap<>();
        }
        HashMap<String, List<PositionInfo>> indexHashMap = new HashMap<>();
        ByteBuffer byteBuffer = mmapFile.getByteBuffer();
        byteBuffer.position(0);
        int wPosition = byteBuffer.getInt();
        log.info("readAllIndex wPosition:{}, indexPath:{}", wPosition, indexFilePath);
        while (byteBuffer.hasRemaining()) {
            int keyLength = byteBuffer.getInt();
            if (keyLength <= 0) break;
            byte[] bytes = new byte[keyLength];
            byteBuffer.get(bytes);
            String key = new String(bytes, StandardCharsets.UTF_8);
            int count = byteBuffer.getInt();
            for (int i = 0; i < count; i++) {
                int position = byteBuffer.getInt();
                int keyL = byteBuffer.getInt();
                int valL = byteBuffer.getInt();
                indexHashMap.computeIfAbsent(key, newKey -> new ArrayList<>()).add(new PositionInfo(position, keyL, valL));
            }
        }
       return indexHashMap;
    }

    public boolean rewriteIndexToFile() {
        for (String filePath : indexMap.keySet()) {
            HashMap<String, Set<PositionInfo>> positionMap = indexMap.get(filePath);
            boolean res = writeIndexToFile(positionMap, filePath);
            log.info("rewriteIndexToFile path:{}, res:{}", filePath, res);
        }
        return true;
    }
}
