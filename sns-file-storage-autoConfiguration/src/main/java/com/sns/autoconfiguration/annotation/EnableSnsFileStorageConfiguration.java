package com.sns.autoconfiguration.annotation;

import com.sns.autoconfiguration.config.FileStorageConfigMarker;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/7/18 16:46
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import({FileStorageConfigMarker.class})
public @interface EnableSnsFileStorageConfiguration {

}
