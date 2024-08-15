package com.sinux.stream;

import cn.hutool.core.util.IdUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sinux.stream.loader.ProxyClient;
import com.sinux.stream.loader.StreamLoadDataFormat;
import com.sinux.stream.loader.StreamLoadSnapshot;
import com.sinux.stream.loader.StreamLoadUtils;
import com.sinux.stream.properties.StreamLoadProperties;
import com.sinux.stream.properties.StreamLoadTableProperties;
import com.sinux.stream.v2.StreamLoadManagerV2;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

@Slf4j
public class StreamLoadManagerV2Test {

    @Test
    public void demo1() throws JsonProcessingException, InterruptedException, ExecutionException {
        StreamLoadTableProperties loadTableProperties = StreamLoadTableProperties.builder().streamLoadDataFormat(StreamLoadDataFormat.JSON).maxBufferRows(5)
                .database("cdc_test").table("hk_proxy_client_test").chunkLimit(Long.MAX_VALUE).build();

        StreamLoadManagerV2 v2  = new StreamLoadManagerV2(StreamLoadProperties.builder().labelPrefix("sinux")
                .scanningFrequency(50).maxRetries(3)
                .enableTransaction().jdbcUrl("jdbc:mysql://192.168.6.168:9030").username("root").password("")
                .loadUrls("192.168.6.168:8030").defaultTableProperties(loadTableProperties).build(),false);
        String content = new StringBuffer().append(IdUtil.fastSimpleUUID()).append(",").append(IdUtil.fastSimpleUUID()).append(",").append(IdUtil.fastSimpleUUID()).toString();
        v2.init();
        ObjectMapper objectMapper = new ObjectMapper();

      /*  objectMapper.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            @Override
            public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeString("");
            }
        });*/
        ProxyClient client = new ProxyClient();
        long start = System.currentTimeMillis();
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.flush();
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
        v2.flush();

        v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),objectMapper.writeValueAsString(new ProxyClient()));
       /* log.info("提交作业开始");
        v2.flush();
        log.info("flush complete");
        StreamLoadSnapshot snapshot = v2.snapshot();
        String shotId = snapshot.getId();

        log.info("snapshot:{}",shotId);
        log.info("prepare:{}",v2.prepare(snapshot));
        log.info("commit:{}",v2.commit(snapshot));

        log.info("提交作业开始");
        v2.flush();
        log.info("flush complete");
        log.info("snapshot:{}",snapshot.getId());
        log.info("prepare:{}",v2.prepare(snapshot));
        log.info("commit:{}",v2.commit(snapshot));*/

        log.info("提交作业开始");
        v2.flush();
        StreamLoadSnapshot snapshot = v2.snapshot();
     /*   for (StreamLoadSnapshot.Transaction transaction : snapshot.getTransactions()) {
            transaction.getLabel()
        }*/
        log.info("flush complete");
        log.info("snapshot:{}",snapshot.getId());
        log.info("prepare:{}",v2.prepare(snapshot));
        log.info("commit:{}",v2.commit(snapshot));

        /*for(int i=0;i<3;i++){
            ThreadUtil.execAsync(()->{
                synchronized (Object.class){

                }
            }).get();
        }*/
        //StreamLoadSnapshot snapshot = v2.snapshot();
      /*  v2.prepare(snapshot);
        v2.commit(snapshot);*/
      //  Thread.sleep(10000);
    //   Thread.sleep(1000000);
        StreamLoadSnapshot snapshot2 = v2.snapshot();
        log.info("cost:{}ms",System.currentTimeMillis()-start);
        /*
        for(;;){
            ProxyClient client = new ProxyClient();
            String json = objectMapper.writeValueAsString(client);
            v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),json);
            Thread.sleep(1000);
            v2.prepare(v2.snapshot());
            v2.commit(v2.snapshot());
        }*/

  //      v2.flush();
        //  starrocksWriteManager.write("cdc_test","hk_proxy_client_test",objectMapper.writeValueAsString(client2));
    //    v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),json);
      /*  v2.flush();
        v2.prepare(v2.snapshot());
        v2.commit(v2.snapshot());
        Thread.sleep(10000);*/
      //  v2.flush();
    }
}
