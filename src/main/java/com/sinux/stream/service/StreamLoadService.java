package com.sinux.stream.service;

import cn.hutool.core.util.IdUtil;
import com.sinux.stream.loader.StreamLoadManager;
import com.sinux.stream.loader.StreamLoadResponse;
import com.sinux.stream.loader.StreamLoadSnapshot;
import com.sinux.stream.loader.TableRegion;
import com.sinux.stream.loader.TransactionStreamLoader;
import com.sinux.stream.properties.StreamLoadProperties;
import lombok.Builder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamLoadService implements StreamLoadManager {
    private static  Map<String, List<TableRegion>> regionMap = new ConcurrentHashMap<>();
    private static  ThreadLocal<TableRegionTx> TX_THREAD_LOCAL = new ThreadLocal<>();
    private TransactionStreamLoader streamLoader;
    public StreamLoadService(StreamLoadProperties loadProperties) {
        this.streamLoader = new TransactionStreamLoader();
        this.streamLoader.start(loadProperties,this);
    }
    private String begin(){
        TX_THREAD_LOCAL.set(TableRegionTx.builder().regions(new ArrayList<>()).txId(IdUtil.fastSimpleUUID()).build());
        return "success";
    }
    public String write(byte[] rows,String dataBase,String table){

        return "";
    }

    public void rollBack(String txId){
        TX_THREAD_LOCAL.remove();
    }
    public void prepareCommit(String txId){

    }
    public void commit(String txId){
        TX_THREAD_LOCAL.remove();

    }

    @Override
    public void init() {

    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {

    }

    @Override
    public void callback(StreamLoadResponse response, String tableId) {

    }

    @Override
    public void callback(Throwable e) {

    }

    @Override
    public void flush() {

    }

    @Override
    public StreamLoadSnapshot snapshot() {
        return null;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public void close() {
        TX_THREAD_LOCAL=null;
        regionMap=null;
    }

    @Builder
    public static final class TableRegionTx implements Serializable{
        private String txId;
        private List<TableRegion> regions;
    }
}
