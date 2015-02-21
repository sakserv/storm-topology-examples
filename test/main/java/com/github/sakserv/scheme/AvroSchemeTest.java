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
package com.github.sakserv.scheme;

import mypipe.avro.InsertMutation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AvroSchemeTest {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemeTest.class);
    
    //@Test
    public void testAvroScheme() {

        byte[] byteArray = {12, 109, 121, 112, 105, 112, 101, 14, 116, 101, 115, 116, 105, 110,
                103, -96, 1, 4, 4, 105, 100, -38, 19, 10, 115, 99, 111, 114, 101, -104, 1, 0, 6, 14, 115,
                117, 98, 106, 101, 99, 116, 14, 98, 105, 111, 108, 111, 103, 121, 16, 108, 97, 115, 116, 110,
                97, 109, 101, 10, 102, 108, 101, 99, 107, 18, 102, 105, 114, 115, 116, 110, 97, 109, 101, 10, 107,
                97, 116, 104, 121, 0};
    
            // deserialize the payload using the appropriate avro schema
            String deserializedValue = "";
            try {
                
                Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("InsertMutation.avsc"));
                LOG.info("SCHEMA: " + schema.toString(true));

                GenericDatumReader<Object> reader =
                        new GenericDatumReader<Object>(schema);
                
                Decoder decoder = DecoderFactory.get().binaryDecoder(byteArray, null);
                Object datum = reader.read(null, decoder);

                LOG.info("STRINGS: " + datum.toString());
            } catch (IOException e) {
                e.printStackTrace();
                deserializedValue = e.getMessage();
            }
            LOG.info("VALUE: " + deserializedValue);
    } 
}
