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

import java.util.Arrays;
import java.util.List;

public class AvroScheme implements Scheme {

        private static final long serialVersionUID = -2990121166902741545L;

        @Override
        public List<Object> deserialize(byte[] bytes) {
            String protocolVersion = Byte.toString(bytes[0]);
            String mutationTypeIdx = Byte.toString(bytes[1]);
            String mutationType = "";
            switch (mutationTypeIdx) {
                case "1":   mutationType = "insert"; break;
                case "2":   mutationType = "update"; break;
                case "3":   mutationType = "delete"; break;
            }
            
            
            byte[] newByteArray = Arrays.copyOfRange(bytes, 2, bytes.length - 1);
            String theRest = Arrays.toString(newByteArray);
            
            return new Values(protocolVersion, mutationType, theRest);
        }

        @Override
            public Fields getOutputFields() {
            return new Fields("version", "type", "theRest");
        }
}
