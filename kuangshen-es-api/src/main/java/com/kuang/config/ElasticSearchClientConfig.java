package com.kuang.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// 1.找对象
// 2.放到 Spring 中待使用
// 3.先分析源码
// xxxAutoConfiguration xxxProperties
@Configuration // xml - bean
public class ElasticSearchClientConfig {

    // spring <bean id="restHighLevelClient" class="RestHighLevelClient" />
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"))); // 集群配置多个，逗号隔开即可
        return client;
    }
}
