package com.pingan.xjl.es.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * es 配置类
 * @author Aaron
 * @date 2020/5/8 23:36
 */
@ConfigurationProperties(prefix = "search.es")
public class RestClientProperties {

    public RestClientProperties() {}

    private String address = "http://localhost:9200";

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

}
