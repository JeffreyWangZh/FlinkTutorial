package com.jw9j.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器温度读数类
 * @Author jw9j
 * @create 2021/7/7 1:01
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long timeStamp;
    private Double temperature;
}
