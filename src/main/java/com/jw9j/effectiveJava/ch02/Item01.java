package com.jw9j.effectiveJava.ch02;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Random;

/**
 * @Author jw9j
 * @create 2021/7/26 22:01
 */
public class Item01 {
    public static void main(String[] args) {
        // 静态工厂方法
        BigInteger.probablePrime(1,new Random());

    }
}
