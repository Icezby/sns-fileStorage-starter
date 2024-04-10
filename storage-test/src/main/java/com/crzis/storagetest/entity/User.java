package com.crzis.storagetest.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/3/30 14:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class User {

    private Integer id;
    private String name;
    private char isBoy;
}
