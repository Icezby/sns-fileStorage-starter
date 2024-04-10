package com.crzis.storagetest.controller;

import com.alibaba.fastjson.JSON;
import com.sns.autoconfiguration.entity.PositionInfo;
import com.sns.autoconfiguration.entity.StorageEntity;
import com.sns.autoconfiguration.storage.DefaultFileStorage;
import com.sns.autoconfiguration.storage.MmapFile;
import com.crzis.storagetest.entity.User;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/7/19 16:35
 */
@Slf4j
@RestController
@RequestMapping("/file")
public class FileController {

    @Autowired
    private DefaultFileStorage<User> fileStorage;

    @RequestMapping("/addData")
    public String addData(@RequestBody Set<StorageEntity<User>> dataInfoList) {
        if (CollectionUtils.isEmpty(dataInfoList)) {
            return "dataInfoList is empty";
        }
        log.info("addData dataInfoList:{}", JSON.toJSONString(dataInfoList));
        boolean writeToFile = fileStorage.putData(dataInfoList);
        return "result: " + writeToFile;
    }

    @RequestMapping("/findByKey/{key}")
    public String findByKey(@PathVariable String key) {
        if (StringUtils.isBlank(key)) {
            return "key is null";
        }
        StorageEntity<User> storageEntity = fileStorage.getData(key);
        return JSON.toJSONString(storageEntity);
    }

    @RequestMapping("/readAllSet")
    public String readAllSet(String path) {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        Set<StorageEntity<User>> storageEntities = fileStorage.readFileAllSet(path);
        return JSON.toJSONString(storageEntities);
    }

    @RequestMapping("/readAllList")
    public String readAllList(String path) {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        List<StorageEntity<User>> storageEntities = fileStorage.readFileAllList(path);
        return JSON.toJSONString(storageEntities);
    }

    @RequestMapping("/arrangeFile")
    public String arrangeFile() {
        fileStorage.arrangeFileData();
        return "success";
    }

    @RequestMapping("/deleteFile")
    public String deleteFile(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        MmapFile.unload(path);
        log.info("mmap:{}", MmapFile.mmapFileMap.get(path));
        Files.delete(Paths.get(path));

        return "ok";
    }

    @RequestMapping("/renameFile")
    public String renameFile(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        File file = new File(path);
        MmapFile.unload(path);
        File newFile = new File(path + ".temp");
        boolean renameOk = file.renameTo(newFile);

        return "result: " + renameOk;
    }

    @RequestMapping("/moveFile")
    public String moveFile(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        Path sourcePath = Paths.get(path);
        MmapFile.unload(path);
        Path target = Paths.get(path + ".temp");
        Path move = Files.move(sourcePath, target, StandardCopyOption.REPLACE_EXISTING);

        return "result: " + move;
    }

    @RequestMapping("/readIndexFile")
    public String readIndexFile(String path) {
        if (StringUtils.isBlank(path)) {
            return "path is null";
        }
        HashMap<String, List<PositionInfo>> stringSetHashMap = fileStorage.readAllIndex(path);
        return JSON.toJSONString(stringSetHashMap);
    }

    @RequestMapping("/getIndexMap")
    public String getIndexMap(String path) {
        if (StringUtils.isBlank(path)) {
            return null;
        }
        HashMap<String, Set<PositionInfo>> map = fileStorage.indexMap.get(path);
        return JSON.toJSONString(map);
    }

    @RequestMapping("/getCurrentData")
    public String getCurrentData(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        Set<StorageEntity<User>> entitySet = fileStorage.currentData.get(key);
        return JSON.toJSONString(entitySet);
    }

    @RequestMapping("/writeIndexToFile")
    public String writeIndexToFile() {
        Map<String, Set<PositionInfo>> indexInfoMap = new HashMap<>();
        Set<PositionInfo> set = new HashSet<>();
        set.add(new PositionInfo(0, 4, 192));
        indexInfoMap.put("1002", set);

        String filePath = "D:/file_db/db.11.data";
        return "result: " + fileStorage.writeIndexToFile(indexInfoMap, filePath);
    }

    @RequestMapping("/rewriteIndexToFile")
    public String rewriteIndexToFile() {
        boolean b = fileStorage.rewriteIndexToFile();
        return "result: " + b;
    }

    @RequestMapping("/getMmapFile")
    public String getMmapFile() {
        StringBuilder sb = new StringBuilder();
        Map<String, MmapFile> mmapFileMap = MmapFile.mmapFileMap;
        for (Entry<String, MmapFile> entry : mmapFileMap.entrySet()) {
            sb.append(entry.getKey()).append("_")
                .append(entry.getValue().getByteBuffer())
                .append(entry.getValue())
                .append(";;;;");
        }
        return sb.toString();
    }

    @RequestMapping("/deleteData")
    public String deleteData(String key) {
        if (StringUtils.isBlank(key)) {
            return "key is null";
        }
        fileStorage.delData(key);
        return "ok";
    }
}
