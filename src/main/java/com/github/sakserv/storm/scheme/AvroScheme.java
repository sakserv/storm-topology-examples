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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AvroScheme implements Scheme {

    //
    private static final Logger LOG = LoggerFactory.getLogger(AvroScheme.class);

    private static final long serialVersionUID = -2990121166902741545L;
    
        // 1 byte - magic for version
        // 1 byte - mutation id
        // n bytes - schemaid (this appears to always be a short with a val of 0)
        // n bytes - payload

        @Override
        public List<Object> deserialize(byte[] bytes) {
            
            // magic version byte and mutation type id
            String protocolVersion = Byte.toString(bytes[0]);
            String mutationTypeIdx = Byte.toString(bytes[1]);
            String mutationType = "";
            switch (mutationTypeIdx) {
                case "1":   mutationType = "insert"; break;
                case "2":   mutationType = "update"; break;
                case "3":   mutationType = "delete"; break;
            }
            
            // Remove first two bytes
            byte[] schemaIdByteArray = Arrays.copyOfRange(bytes, 2, 4);

            // TODO: Find out if all schemaIds are a short with val 0
/*            ByteBuffer bb = ByteBuffer.allocate(2);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.put(newByteArray[0]);
            bb.put(newByteArray[1]);
            short schemaId = bb.getShort(0);*/

            // Get the payload bytes
            byte[] payloadByteArray = Arrays.copyOfRange(bytes, 4, bytes.length - 1);
            
            // deserialize the payload using the appropriate avro schema
            String deserializedValue = "";
            try {
                deserializedValue = AvroSchemaUtils.deserializeInsertMutation(payloadByteArray);
            } catch (IOException e) {
                LOG.info("ERROR: Failed to deserialize avro byte array for InsertMutation");
                deserializedValue = "exception";
            }
                      
            return new Values(protocolVersion, mutationType, deserializedValue);
        }

        @Override
            public Fields getOutputFields() {
            return new Fields("version", "type", "deserializedValue");
        }
}
