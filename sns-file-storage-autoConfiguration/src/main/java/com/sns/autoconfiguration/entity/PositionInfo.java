package com.sns.autoconfiguration.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/4/12 15:47
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class PositionInfo {
    private int position;
    private int keyLength;
    private int valueLength;
}
