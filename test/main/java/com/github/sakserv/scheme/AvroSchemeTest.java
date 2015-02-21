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

import com.github.sakserv.avro.AvroSchemaUtils;
import com.github.sakserv.avro.InsertMutation;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AvroSchemeTest {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemeTest.class);
    
    @Test
    public void testAvroScheme() {

        byte[] byteArray = {12, 109, 121, 112, 105, 112, 101, 14, 116, 101, 115, 116, 105, 110, 103, -96, 
                1, 4, 4, 105, 100, 18, 10, 115, 99, 111, 114, 101, -56, 1, 0, 8, 14, 115, 117, 98, 106, 101, 
                99, 116, 16, 114, 101, 108, 105, 103, 105, 111, 110, 16, 108, 97, 115, 116, 110, 97, 109, 
                101, 10, 115, 116, 97, 109, 109, 18, 102, 105, 114, 115, 116, 110, 97, 109, 101, 14, 114, 
                111, 115, 97, 110, 110, 101, 8, 100, 97, 116, 101, 20, 50, 48, 49, 52, 45, 49, 48, 45, 50, 50, 0, 0};
    

            try {
                SpecificDatumReader<InsertMutation> reader = new SpecificDatumReader<>(InsertMutation.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(byteArray, null);
                InsertMutation insertMutation = reader.read(null, decoder);

                LOG.info("ENTRY: " + insertMutation.toString());
                
                LOG.info("DATABASE NAME: " + insertMutation.getDatabase());
                assertEquals("mypipe", insertMutation.getDatabase().toString());
                
                LOG.info("TABLE NAME: " + insertMutation.getTable());
                assertEquals("testing", insertMutation.getTable().toString());
                
                LOG.info("ID: " + AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "id"));
                assertEquals(9, (int) AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "id"));

                LOG.info("FIRST NAME: " + AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "firstname"));
                assertEquals("rosanne", AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "firstname"));
                
                LOG.info("LAST NAME: " + AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "lastname"));
                assertEquals("stamm", AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "lastname"));
                
                LOG.info("SUBJECT: " + AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "subject"));
                assertEquals("religion", AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "subject"));
                
                LOG.info("SCORE: " + AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "score"));
                assertEquals(100, (int) AvroSchemaUtils.getIntegerValueByKey(insertMutation.getIntegers(), "score"));
                
                LOG.info("DATE: " + AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "date"));
                assertEquals("2014-10-22", AvroSchemaUtils.getStringValueByKey(insertMutation.getStrings(), "date"));

            } catch (IOException e) {
                e.printStackTrace();
            }
    } 
}
