package com.pingan.xjl.es.config;

import cn.hutool.core.util.NumberUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;

/**
 *  restClient 自动装配类
 * @author Aaron
 * @date 2020/4/29 22:17
 */
@Configuration(proxyBeanMethods=false)
@EnableConfigurationProperties(RestClientProperties.class)
public class HighLevelClientAutoConfig {

    private static final String SPLIT = ",";

    @Resource
    private RestClientProperties properties;

    public HighLevelClientAutoConfig(){}

    /**
     * 构建 RestHighLevelClient
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(name="restHighLevelClient")
    RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        splitAddressToHosts(properties.getAddress())));
        return client;
    }

    public HttpHost[] splitAddressToHosts(String address) {
        if (StringUtils.isEmpty(address)) {
            return new HttpHost[0];
        } else {
            String[] adds ;
            if (address.contains(SPLIT)) {
                adds = address.split(SPLIT);
            } else {
                adds = new String[]{address};
            }
            HttpHost[] hosts = new HttpHost[adds.length];
            for (int i = 0; i < adds.length; i++) {
                String add = adds[i];
                if (!add.contains("://") || !add.contains(":")) {
                    throw new RuntimeException("es 地址格式错误 ，正确的格式为 'http://host1:port1,http://host2:port2' ");
                }
                String scheme = add.split("://")[0];
                if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                    throw new RuntimeException("es 地址格式错误 ，只支持http或者https协议！");
                }
                String portStr = add.substring(add.lastIndexOf(":")+1);
                if (!NumberUtil.isInteger(portStr)) {
                    throw new RuntimeException("es 地址格式错误 ，端口号必须为数字！！！！！ ");
                }
                int port = Integer.valueOf(portStr);
                String host = add.substring(add.indexOf("://")+3,add.lastIndexOf(":"));
                hosts[i] = new HttpHost(host,port,scheme);
            }
            return hosts;
        }
    }

}
