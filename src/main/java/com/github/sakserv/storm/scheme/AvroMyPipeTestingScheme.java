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
import mypipe.avro.InsertMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AvroMyPipeTestingScheme implements Scheme {

    //
    private static final Logger LOG = LoggerFactory.getLogger(AvroMyPipeTestingScheme.class);

    private static final long serialVersionUID = -2990121166902741545L;
    
    List<String> fieldsList = new ArrayList<String>();
    
        // 1 byte - magic for version
        // 1 byte - mutation id
        // n bytes - schemaid (this appears to always be a short with a val of 0)
        // n bytes - payload

        @Override
        public List<Object> deserialize(byte[] bytes) {
            
            Values values = new Values();
            
            String mutationType = AvroSchemaUtils.getMutationType(bytes);
            String firstName = "";
            if(mutationType.equals("InsertMutation")) {
                byte[] payload = AvroSchemaUtils.getAvroPayload(bytes);
                try {
                    InsertMutation insertMutation = AvroSchemaUtils.deserializeInsertMutation(payload);
                    
                    // FirstName
                    fieldsList.add("firstname");
                    /*values.add(AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "firstname"));*/
                    firstName = AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "firstname");
                    
                } catch(IOException e) {
                    e.printStackTrace();
                }

            }

            return new Values(firstName);
        }

        @Override
            public Fields getOutputFields() {
            return new Fields("firstName");
        }
}
