package com.sns.autoconfiguration.storage;

import com.sns.autoconfiguration.utils.FileUtil;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

@Slf4j
public class MmapFile {

    private String filePath;
    private long initialBufferSize;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private volatile int writePosition;

    public static final Map<String, MmapFile> mmapFileMap = new ConcurrentHashMap<>(1000);

    public static MmapFile getOrLoad(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        MmapFile mmapFile = mmapFileMap.get(filePath);
        if (null == mmapFile) {
            log.info("debug###getOrLoad mmapFile is null start create,filePath：{}", filePath);
            synchronized (filePath.intern()) {
                mmapFile = mmapFileMap.get(filePath);
                if (null == mmapFile) {
                    if (!FileUtil.exist(filePath)) {
                        return null;
                    }
                    File file = new File(filePath);
                    long initialBufferSize = file.length();
                    mmapFile = new MmapFile(filePath, initialBufferSize);
                    mmapFileMap.put(filePath, mmapFile);
                }
            }
        }
        return mmapFile;
    }

    public static MmapFile getOrCreate(String filePath, long initialBufferSize, float resizeFactor) {
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        MmapFile mmapFile;
        try{
            mmapFile = mmapFileMap.get(filePath);
            if (null == mmapFile) {
                log.info("debug###getOrCreate mmapFile is null start create,filePath：{}", filePath);
                synchronized (filePath.intern()) {
                    mmapFile = mmapFileMap.get(filePath);
                    if (null == mmapFile) {
                        mmapFile = new MmapFile(filePath, initialBufferSize);
                        mmapFileMap.put(filePath, mmapFile);
                    }
                    return mmapFile;
                }
            }
        }catch (Exception e) {
            log.error("MmapFile getOrCreate error.", e);
            return null;
        }
        return resize(mmapFile, initialBufferSize, filePath, resizeFactor);
    }

    private long computeInitSize(long initialBufferSize) {
        // 初始给64k， 每次扩容乘以2
        long initSize = 1024 * 1L;
        if (initialBufferSize <= initSize) {
            return initSize;
        }
        while (initSize <= initialBufferSize) {
            initSize = initSize << 1;
        }
        return initSize;
    }

    /**
     * 计算扩容
     * @param mmapFile
     * @param needSize
     * @return
     */
    public static MmapFile resize(MmapFile mmapFile, long needSize, String filePath, float resizeFactor) {
        if (Objects.isNull(mmapFile)) {
            return null;
        }
        long start = System.currentTimeMillis();
        synchronized (filePath.intern()) {
            ByteBuffer byteBuffer = mmapFile.getByteBuffer();
            int limit = byteBuffer.limit();
            float v = ((mmapFile.writePosition + needSize) * 1f) / limit;;
            log.info("debug###装载率 byteBuffer:{}, writePosition:{}, v:{}, filePath:{}", byteBuffer, mmapFile.writePosition, v, filePath);
            if (v < resizeFactor) {
                return mmapFile;
            }
            long newSize = Integer.valueOf(limit).longValue();
            do {
                if (newSize > (Integer.MAX_VALUE >> 1)) {
                    newSize = Integer.MAX_VALUE;
                    if ((newSize - Integer.valueOf(mmapFile.writePosition).longValue()) < needSize) {
                        // todo 可调用整理方法
                        throw new RuntimeException("data too large, file now size MaxValue, writePosition：" + mmapFile.writePosition + ", needSize: " + needSize);
                    }
                }else {
                    newSize = newSize << 1;
                }
            }while ((newSize - Integer.valueOf(mmapFile.writePosition).longValue()) < needSize);
            try {
                mmapFile.setByteBuffer(mmapFile.fileChannel.map(MapMode.READ_WRITE, 0, newSize));
            } catch (IOException e) {
                log.error("file resize error, filePath:{}, newSize:{}, wPosition:{} needSize:{}, exception:", filePath, newSize, needSize, mmapFile.writePosition, e);
            }
            log.info("debug###扩容结束,byteBuffer:{}, cost:{}", mmapFile.getByteBuffer(), System.currentTimeMillis() - start);
            return mmapFile;
        }
    }

    public MmapFile(String filePath, long initialBufferSize) {
        long initSize = computeInitSize(initialBufferSize);
        this.initialBufferSize = initSize;
        this.filePath = filePath;
        try {
            randomAccessFile = new RandomAccessFile(this.filePath, "rw");
            fileChannel = randomAccessFile.getChannel();
            byteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, initSize);
            int wPosition = byteBuffer.getInt();
            log.info("MmapFile get write position:{}, filePath:{}, byteBuffer:{}", wPosition, filePath, byteBuffer);
            this.writePosition = wPosition > 0 ? wPosition : Integer.BYTES;
        }catch (Exception e) {
            log.error("filePath="+filePath+", initialBufferSize="+initSize, e);
        }
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public static void unload(String filePath) {
        MmapFile mmapFile = mmapFileMap.get(filePath);
        if (null != mmapFile) {
            mmapFile.close();
            mmapFile.unmap();
            mmapFileMap.remove(filePath);
            log.info("unload, filePath = {}, mmap:{}", filePath, mmapFile);
        }
    }

    public void close(){
        try {
            ((MappedByteBuffer)this.byteBuffer).force();
            fileChannel.close();
            randomAccessFile.close();
        }catch (Exception e) {
            log.error("", e);
        }
    }

    public void unmap() {
        unmap(this.byteBuffer);
    }

    private void unmap(ByteBuffer bb) {
        Cleaner cl = ((DirectBuffer)bb).cleaner();
        if (cl != null)
            cl.clean();
    }

    public long getInitialBufferSize() {
        return initialBufferSize;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setInitialBufferSize(long initialBufferSize) {
        this.initialBufferSize = initialBufferSize;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public int getWritePosition() {
        return writePosition;
    }

    public void setWritePosition(int writePosition) {
        this.writePosition = writePosition;
    }


}
