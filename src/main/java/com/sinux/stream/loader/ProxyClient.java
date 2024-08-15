package com.sinux.stream.loader;

import cn.hutool.core.util.IdUtil;
import lombok.Data;

import java.io.Serializable;

@Data
public class ProxyClient implements Serializable {
    private String id;
    private String user_id;
    private String client_key;


    public ProxyClient() {
        this.id= IdUtil.fastSimpleUUID();
        this.user_id= IdUtil.fastSimpleUUID();
        this.client_key= IdUtil.fastSimpleUUID();
    }
}
