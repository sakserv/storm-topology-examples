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
package com.github.sakserv.avro;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import mypipe.avro.InsertMutation;

import java.io.IOException;
import java.util.Map;

public class AvroSchemaUtils {
    
    public Class schemaClassFromString(String schemaName) {
        Class clsName = null;
        switch(schemaName) {
            case "insert":  clsName = InsertMutation.class; break;
            case "update":  clsName = UpdateMutation.class; break;
            case "delete":  clsName =  DeleteMutation.class; break;
        }
        return clsName;
    }
    
    public static String deserializeInsertMutation(byte[] bytes) throws IOException {
        SpecificDatumReader<InsertMutation> reader =
                new SpecificDatumReader<InsertMutation>(InsertMutation.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        InsertMutation insertMutation = reader.read(null, decoder);
        return insertMutation.toString();
    }

    public Integer getIntegerValueByKey(Map<CharSequence, Integer> map, String key) {
        for(Map.Entry<CharSequence, Integer> entry: map.entrySet()) {
            if (entry.getKey().toString().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public String getStringValueByKey(Map<CharSequence, CharSequence> map, String key) {
        for(Map.Entry<CharSequence, CharSequence> entry: map.entrySet()) {
            if (entry.getKey().toString().equals(key)) {
                return entry.getValue().toString();
            }
        }
        return null;
    }
    
}
