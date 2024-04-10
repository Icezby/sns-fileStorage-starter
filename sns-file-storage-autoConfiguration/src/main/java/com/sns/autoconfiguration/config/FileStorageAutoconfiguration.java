package com.sns.autoconfiguration.config;

import com.sns.autoconfiguration.storage.DefaultFileStorage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/7/18 17:39
 */
@Configuration
@EnableConfigurationProperties(StorageConfig.class)
@ConditionalOnBean(FileStorageConfigMarker.class)
public class FileStorageAutoconfiguration {

    @Bean
    public DefaultFileStorage getDefaultStorageService(StorageConfig storageConfig) {
        return new DefaultFileStorage<>(storageConfig);
    }
}
