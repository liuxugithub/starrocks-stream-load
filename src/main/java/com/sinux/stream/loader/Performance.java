package com.sinux.stream.loader;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sinux.stream.properties.StreamLoadProperties;
import com.sinux.stream.properties.StreamLoadTableProperties;
import com.sinux.stream.v2.StreamLoadManagerV2;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Performance {
    public static final StreamLoadTableProperties loadTableProperties = StreamLoadTableProperties.builder().streamLoadDataFormat(StreamLoadDataFormat.JSON).maxBufferRows(3500)
            .database("cdc_test").table("hk_proxy_client_test").chunkLimit(Long.MAX_VALUE).build();
    public static final   StreamLoadManagerV2 v2  = new StreamLoadManagerV2(StreamLoadProperties.builder().labelPrefix("sinux")
            .scanningFrequency(50).maxRetries(3)
            .enableTransaction().jdbcUrl("jdbc:mysql://192.168.6.168:9030").username("root").password("")
            .loadUrls("192.168.6.168:8030").defaultTableProperties(loadTableProperties).build(),false);
    public static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        v2.init();
    }
    public static void main(String[] args) throws JsonProcessingException {
        Integer batchCount = Integer.valueOf(args[0]);
        Integer allCount = Integer.valueOf(args[1]);
        List<String> allData = new ArrayList<>();
        for(int i=0;i<allCount;i++){
            allData.add(objectMapper.writeValueAsString(new ProxyClient()));
        }
        List<List<String>> splitDatas = CollectionUtil.split(allData,batchCount);
        long start0 = System.currentTimeMillis();
        for(List<String> datas:splitDatas){
            String[] strs =  ArrayUtil.toArray(datas,String.class);
            //long start = System.currentTimeMillis();
            v2.write(StreamLoadUtils.getTableUniqueKey(loadTableProperties.getDatabase(),loadTableProperties.getTable()),loadTableProperties.getDatabase(),loadTableProperties.getTable(),strs);
            v2.flush();
            //log.info("batch insert cost:{}",System.currentTimeMillis()-start);
        }

        StreamLoadSnapshot snapshot = v2.snapshot();
        v2.prepare(snapshot);
        v2.commit(snapshot);
        log.info("insert {},lines, cost:{}",allCount,System.currentTimeMillis()-start0);
    }
}
