package com.pingan.xjl.es;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Es highlevelClient demo
 * @author Aaron shaw
 * @date 2020/05/03
 *
 */
@SpringBootApplication
public class XjlSearchEsApplication {

    public static void main(String[] args) {
        SpringApplication.run(XjlSearchEsApplication.class, args);
        System.out.println("es search 服务启动成功！！！！！！！！！！！！！！！！！！！！");
    }

}
