/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import java.util.Date;

/**
 * A simple implementation of {@link com.github.sakserv.storm.bolt.MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SimpleMongoUpsertBolt extends MongoUpsertBolt {
    private final String mongoCollectionName;

    /**
     * @param mongoHost The host on which Mongo is running.
     * @param mongoPort The port on which Mongo is running.
     * @param mongoDbName The Mongo database containing all collections being
     * written to.
     * @param mongoCollectionName The Mongo collection to write to. If a
     * collection with this name does not already exist, it will be
     * automatically created.
     */
    public SimpleMongoUpsertBolt(
            String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName) {

        super(mongoHost, mongoPort, mongoDbName);
        this.mongoCollectionName = mongoCollectionName;
    }


    @Override
    public boolean shouldActOnInput(Tuple input) {
        return true;
    }

    @Override
    public String getMongoCollectionForInput(Tuple input) {
        return mongoCollectionName;
    }

    @Override
    public DBObject getDBObjectForInput(Tuple input) {
        BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();

        for (String field : input.getFields()) {
            Object value = input.getValueByField(field);
            if (isValidDBObjectField(value)) {
                dbObjectBuilder.append(field, value);
            }
        }

        return dbObjectBuilder.get();
    }

    private boolean isValidDBObjectField(Object value) {
        return value instanceof String
                || value instanceof Date
                || value instanceof Integer
                || value instanceof Float
                || value instanceof Double
                || value instanceof Short
                || value instanceof Long
                || value instanceof DBObject;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
