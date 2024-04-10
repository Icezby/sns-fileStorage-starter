package com.crzis.storagetest;

import com.sns.autoconfiguration.annotation.EnableSnsFileStorageConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableSnsFileStorageConfiguration
public class StorageTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(StorageTestApplication.class, args);
    }

}
