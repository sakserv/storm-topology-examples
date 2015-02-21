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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class AvroSchemaUtils {
    
    public static String getMutationType(byte[] bytes) {

        String mutationTypeIdx = Byte.toString(bytes[1]);
        String mutationType = "";
        switch (mutationTypeIdx) {
            case "1":   mutationType = "InsertMutation"; break;
            case "2":   mutationType = "UpdateMutation"; break;
            case "3":   mutationType = "DeleteMutation"; break;
        }
        return mutationType;
    }
    
    public static byte[] getAvroPayload(byte[] bytes) {
        return Arrays.copyOfRange(bytes, 4, bytes.length);
    }
    
    public static InsertMutation deserializeInsertMutation(byte[] bytes) throws IOException {
        SpecificDatumReader<InsertMutation> reader =
                new SpecificDatumReader<InsertMutation>(InsertMutation.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        InsertMutation insertMutation = reader.read(null, decoder);
        return insertMutation;
    }

    public static Integer getIntegerValueByKey(Map<CharSequence, Integer> map, String key) {
        for(Map.Entry<CharSequence, Integer> entry: map.entrySet()) {
            if (entry.getKey().toString().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static String getStringValueByKey(Map<CharSequence, CharSequence> map, String key) {
        for(Map.Entry<CharSequence, CharSequence> entry: map.entrySet()) {
            if (entry.getKey().toString().equals(key)) {
                return entry.getValue().toString();
            }
        }
        return null;
    }
    
}
