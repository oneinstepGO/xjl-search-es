package com.pingan.xjl.es.config;

/**
 * @author Aaron
 * @date 2020/5/9 0:10
 */
public class Test {

    public static void main(String[] args) {
        String add = "http://localhost:9200";
        System.out.println(add.substring(add.indexOf("://")+3,add.lastIndexOf(":")));
    }
}
