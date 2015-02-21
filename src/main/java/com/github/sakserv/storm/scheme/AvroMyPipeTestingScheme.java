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
package com.github.sakserv.storm.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.github.sakserv.avro.AvroSchemaUtils;
import com.github.sakserv.avro.DeleteMutation;
import com.github.sakserv.avro.InsertMutation;
import com.github.sakserv.avro.UpdateMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class AvroMyPipeTestingScheme implements Scheme {

    //
    private static final Logger LOG = LoggerFactory.getLogger(AvroMyPipeTestingScheme.class);

    private static final long serialVersionUID = -2990121166902741545L;
    
        // 1 byte - magic for version
        // 1 byte - mutation id
        // n bytes - schemaid (this appears to always be a short with a val of 0)
        // n bytes - payload

        @Override
        public List<Object> deserialize(byte[] bytes) {
            
            Values values = new Values();
            
            String mutationType = AvroSchemaUtils.getMutationType(bytes);
            if(mutationType.equals("InsertMutation")) {
                byte[] payload = AvroSchemaUtils.getAvroPayload(bytes);
                try {
                    InsertMutation insertMutation = AvroSchemaUtils.deserializeInsertMutation(payload);
                    
                    // Database
                    values.add(insertMutation.getDatabase());
                    
                    // Tables
                    values.add(insertMutation.getTable());
                    
                    // Mutation
                    values.add(mutationType);
                    
                    // Id
                    values.add(AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "id"));
                    
                    // FirstName
                    values.add(AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "firstname"));

                    // LastName
                    values.add(AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "lastname"));

                    // subject
                    values.add(AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "subject"));

                    // score
                    values.add(AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "score"));

                    // date
                    values.add(AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "date"));
                    
                } catch(IOException e) {
                    e.printStackTrace();
                }

            } /*else if(mutationType.equals("DeleteMutation")) {
                byte[] payload = AvroSchemaUtils.getAvroPayload(bytes);
                try {
                    DeleteMutation deleteMutation = AvroSchemaUtils.deserializeDeleteMutation(payload);

                    // Database
                    values.add(deleteMutation.getDatabase());

                    // Tables
                    values.add(deleteMutation.getTable());

                    // Mutation
                    values.add(mutationType);

                    // Id
                    values.add(AvroSchemaUtils.getIntegerValueByKey(deleteMutation.getIntegers(), "id"));

                    // FirstName
                    values.add(AvroSchemaUtils.getStringValueByKey(deleteMutation.getStrings(), "firstname"));

                    // LastName
                    values.add(AvroSchemaUtils.getStringValueByKey(deleteMutation.getStrings(), "lastname"));

                    // subject
                    values.add(AvroSchemaUtils.getStringValueByKey(deleteMutation.getStrings(), "subject"));

                    // score
                    values.add(AvroSchemaUtils.getIntegerValueByKey(deleteMutation.getIntegers(), "score"));

                    // date
                    values.add(AvroSchemaUtils.getStringValueByKey(deleteMutation.getStrings(), "date"));

                } catch(IOException e) {
                    e.printStackTrace();
                }

            } else if(mutationType.equals("UpdateMutation")) {
                byte[] payload = AvroSchemaUtils.getAvroPayload(bytes);
                try {
                    UpdateMutation updateMutation = AvroSchemaUtils.deserializeUpdateMutation(payload);

                    // Database
                    values.add(updateMutation.getDatabase());

                    // Tables
                    values.add(updateMutation.getTable());

                    // Mutation
                    values.add(mutationType);

                    // Id
                    values.add(AvroSchemaUtils.getIntegerValueByKey(updateMutation.getNewIntegers(), "id"));

                    // FirstName
                    values.add(AvroSchemaUtils.getStringValueByKey(updateMutation.getNewStrings(), "firstname"));

                    // LastName
                    values.add(AvroSchemaUtils.getStringValueByKey(updateMutation.getNewStrings(), "lastname"));

                    // subject
                    values.add(AvroSchemaUtils.getStringValueByKey(updateMutation.getNewStrings(), "subject"));

                    // score
                    values.add(AvroSchemaUtils.getIntegerValueByKey(updateMutation.getNewIntegers(), "score"));

                    // date
                    values.add(AvroSchemaUtils.getStringValueByKey(updateMutation.getNewStrings(), "date"));

                } catch(IOException e) {
                    e.printStackTrace();
                }

            }*/
            
            return values;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("database", "table", "mutation", "id", "firstname", "lastname", "subject", "score", "date");
        }
}
