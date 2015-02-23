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

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import com.github.sakserv.avro.AvroSchemaUtils;
import com.github.sakserv.avro.InsertMutation;
import scala.actors.threadpool.Arrays;
import storm.kafka.KeyValueScheme;
import storm.kafka.KeyValueSchemeAsMultiScheme;

import java.io.IOException;
import java.util.List;

public class AvroMyPipeTestingKeyValueScheme extends SchemeAsMultiScheme {
    
    //private static byte[] key = new byte[];
    
/*    @Override
    public Iterable<List<Object>> deserialize(byte[] bytes) {
        String mutationType = AvroSchemaUtils.getMutationType(bytes);
        byte[] payload = AvroSchemaUtils.getAvroPayload(bytes);
        
        if(mutationType.equals("InsertMutation")) {
            try {
                InsertMutation insertMutation = AvroSchemaUtils.deserializeInsertMutation(payload);
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
        return Arrays.asList(new String[]);
    }*/
    
    public AvroMyPipeTestingKeyValueScheme(KeyValueScheme scheme) {
        super(scheme);
    }
    
}
