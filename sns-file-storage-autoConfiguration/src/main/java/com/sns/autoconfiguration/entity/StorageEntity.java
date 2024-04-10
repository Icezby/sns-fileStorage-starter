package com.sns.autoconfiguration.entity;

import java.io.Serializable;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: ZhaoBingYang
 * @time: 2023/7/19 10:56
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StorageEntity<T> implements Serializable {
    private String key;
    private Set<T> dataSet;
    private char status = '0';

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StorageEntity<?> that = (StorageEntity<?>) o;

        if (status != that.status) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return dataSet != null ? dataSet.equals(that.dataSet) : that.dataSet == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (dataSet != null ? dataSet.hashCode() : 0);
        result = 31 * result + (int) status;
        return result;
    }
}
