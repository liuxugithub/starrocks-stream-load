/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sinux.stream.loader;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class StreamLoadSnapshot implements Serializable {

    private String id;
    private List<Transaction> transactions;
    private long timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isFinish(String database, String label) {
        for (Transaction transaction : transactions) {
            if (transaction.getDatabase().equals(database) && transaction.getLabel().equals(label)) {
                return transaction.isFinish();
            }
        }
        return false;
    }

    public static class Transaction implements Serializable {
        private String database;
        private String table;
        private String label;
        private boolean finish;

        public Transaction(String database, String table, String label) {
            this.database = database;
            this.table = table;
            this.label = label;
            this.finish = false;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public boolean isFinish() {
            return finish;
        }

        public void setFinish(boolean finish) {
            this.finish = finish;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "database='" + database + '\'' +
                    ", table='" + table + '\'' +
                    ", label='" + label + '\'' +
                    ", finish=" + finish +
                    '}';
        }
    }

    public static StreamLoadSnapshot snapshot(Iterable<TableRegion> regions) {

        List<Transaction> transactions = StreamSupport.stream(regions.spliterator(), false)
                .filter(region -> region.getLabel() != null)
                .map(region -> new Transaction(region.getDatabase(), region.getTable(), region.getLabel()))
                .collect(Collectors.toList());

        StreamLoadSnapshot snapshot = new StreamLoadSnapshot();
        snapshot.setId(UUID.randomUUID().toString());
        snapshot.setTimestamp(System.currentTimeMillis());
        snapshot.setTransactions(transactions);

        return snapshot;
    }


}
