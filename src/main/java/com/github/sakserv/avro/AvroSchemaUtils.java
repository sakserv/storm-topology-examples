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

import java.io.IOException;
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

    public static UpdateMutation deserializeUpdateMutation(byte[] bytes) throws IOException {
        SpecificDatumReader<UpdateMutation> reader =
                new SpecificDatumReader<UpdateMutation>(UpdateMutation.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        UpdateMutation updateMutation = reader.read(null, decoder);
        return updateMutation;
    }

    public static DeleteMutation deserializeDeleteMutation(byte[] bytes) throws IOException {
        SpecificDatumReader<DeleteMutation> reader =
                new SpecificDatumReader<DeleteMutation>(DeleteMutation.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        DeleteMutation deleteMutation = reader.read(null, decoder);
        return deleteMutation;
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

    @SuppressWarnings("all")
    @org.apache.avro.specific.AvroGenerated
    public static class InsertMutation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
        public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"InsertMutation\",\"namespace\":\"mypipe.avro\",\"fields\":[{\"name\":\"database\",\"type\":\"string\"},{\"name\":\"table\",\"type\":\"string\"},{\"name\":\"tableId\",\"type\":\"long\"},{\"name\":\"integers\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"strings\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"longs\",\"type\":{\"type\":\"map\",\"values\":\"long\"}}]}");
        public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
        @Deprecated public CharSequence database;
        @Deprecated public CharSequence table;
        @Deprecated public long tableId;
        @Deprecated public Map<CharSequence,Integer> integers;
        @Deprecated public Map<CharSequence,CharSequence> strings;
        @Deprecated public Map<CharSequence,Long> longs;

        /**
         * Default constructor.  Note that this does not initialize fields
         * to their default values from the schema.  If that is desired then
         * one should use {@link \#newBuilder()}.
         */
        public InsertMutation() {}

        /**
         * All-args constructor.
         */
        public InsertMutation(CharSequence database, CharSequence table, Long tableId, Map<CharSequence, Integer> integers, Map<CharSequence, CharSequence> strings, Map<CharSequence, Long> longs) {
            this.database = database;
            this.table = table;
            this.tableId = tableId;
            this.integers = integers;
            this.strings = strings;
            this.longs = longs;
        }

        public org.apache.avro.Schema getSchema() { return SCHEMA$; }
        // Used by DatumWriter.  Applications should not call.
        public Object get(int field$) {
            switch (field$) {
                case 0: return database;
                case 1: return table;
                case 2: return tableId;
                case 3: return integers;
                case 4: return strings;
                case 5: return longs;
                default: throw new org.apache.avro.AvroRuntimeException("Bad index");
            }
        }
        // Used by DatumReader.  Applications should not call.
        @SuppressWarnings(value="unchecked")
        public void put(int field$, Object value$) {
            switch (field$) {
                case 0: database = (CharSequence)value$; break;
                case 1: table = (CharSequence)value$; break;
                case 2: tableId = (Long)value$; break;
                case 3: integers = (Map<CharSequence,Integer>)value$; break;
                case 4: strings = (Map<CharSequence,CharSequence>)value$; break;
                case 5: longs = (Map<CharSequence,Long>)value$; break;
                default: throw new org.apache.avro.AvroRuntimeException("Bad index");
            }
        }

        /**
         * Gets the value of the 'database' field.
         */
        public CharSequence getDatabase() {
            return database;
        }

        /**
         * Sets the value of the 'database' field.
         * @param value the value to set.
         */
        public void setDatabase(CharSequence value) {
            this.database = value;
        }

        /**
         * Gets the value of the 'table' field.
         */
        public CharSequence getTable() {
            return table;
        }

        /**
         * Sets the value of the 'table' field.
         * @param value the value to set.
         */
        public void setTable(CharSequence value) {
            this.table = value;
        }

        /**
         * Gets the value of the 'tableId' field.
         */
        public Long getTableId() {
            return tableId;
        }

        /**
         * Sets the value of the 'tableId' field.
         * @param value the value to set.
         */
        public void setTableId(Long value) {
            this.tableId = value;
        }

        /**
         * Gets the value of the 'integers' field.
         */
        public Map<CharSequence,Integer> getIntegers() {
            return integers;
        }

        /**
         * Sets the value of the 'integers' field.
         * @param value the value to set.
         */
        public void setIntegers(Map<CharSequence,Integer> value) {
            this.integers = value;
        }

        /**
         * Gets the value of the 'strings' field.
         */
        public Map<CharSequence,CharSequence> getStrings() {
            return strings;
        }

        /**
         * Sets the value of the 'strings' field.
         * @param value the value to set.
         */
        public void setStrings(Map<CharSequence,CharSequence> value) {
            this.strings = value;
        }

        /**
         * Gets the value of the 'longs' field.
         */
        public Map<CharSequence,Long> getLongs() {
            return longs;
        }

        /**
         * Sets the value of the 'longs' field.
         * @param value the value to set.
         */
        public void setLongs(Map<CharSequence,Long> value) {
            this.longs = value;
        }

        /** Creates a new InsertMutation RecordBuilder */
        public static Builder newBuilder() {
            return new Builder();
        }

        /** Creates a new InsertMutation RecordBuilder by copying an existing Builder */
        public static Builder newBuilder(Builder other) {
            return new Builder(other);
        }

        /** Creates a new InsertMutation RecordBuilder by copying an existing InsertMutation instance */
        public static Builder newBuilder(InsertMutation other) {
            return new Builder(other);
        }

        /**
         * RecordBuilder for InsertMutation instances.
         */
        public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<InsertMutation>
                implements org.apache.avro.data.RecordBuilder<InsertMutation> {

            private CharSequence database;
            private CharSequence table;
            private long tableId;
            private Map<CharSequence,Integer> integers;
            private Map<CharSequence,CharSequence> strings;
            private Map<CharSequence,Long> longs;

            /** Creates a new Builder */
            private Builder() {
                super(InsertMutation.SCHEMA$);
            }

            /** Creates a Builder by copying an existing Builder */
            private Builder(Builder other) {
                super(other);
                if (isValidValue(fields()[0], other.database)) {
                    this.database = data().deepCopy(fields()[0].schema(), other.database);
                    fieldSetFlags()[0] = true;
                }
                if (isValidValue(fields()[1], other.table)) {
                    this.table = data().deepCopy(fields()[1].schema(), other.table);
                    fieldSetFlags()[1] = true;
                }
                if (isValidValue(fields()[2], other.tableId)) {
                    this.tableId = data().deepCopy(fields()[2].schema(), other.tableId);
                    fieldSetFlags()[2] = true;
                }
                if (isValidValue(fields()[3], other.integers)) {
                    this.integers = data().deepCopy(fields()[3].schema(), other.integers);
                    fieldSetFlags()[3] = true;
                }
                if (isValidValue(fields()[4], other.strings)) {
                    this.strings = data().deepCopy(fields()[4].schema(), other.strings);
                    fieldSetFlags()[4] = true;
                }
                if (isValidValue(fields()[5], other.longs)) {
                    this.longs = data().deepCopy(fields()[5].schema(), other.longs);
                    fieldSetFlags()[5] = true;
                }
            }

            /** Creates a Builder by copying an existing InsertMutation instance */
            private Builder(InsertMutation other) {
                super(InsertMutation.SCHEMA$);
                if (isValidValue(fields()[0], other.database)) {
                    this.database = data().deepCopy(fields()[0].schema(), other.database);
                    fieldSetFlags()[0] = true;
                }
                if (isValidValue(fields()[1], other.table)) {
                    this.table = data().deepCopy(fields()[1].schema(), other.table);
                    fieldSetFlags()[1] = true;
                }
                if (isValidValue(fields()[2], other.tableId)) {
                    this.tableId = data().deepCopy(fields()[2].schema(), other.tableId);
                    fieldSetFlags()[2] = true;
                }
                if (isValidValue(fields()[3], other.integers)) {
                    this.integers = data().deepCopy(fields()[3].schema(), other.integers);
                    fieldSetFlags()[3] = true;
                }
                if (isValidValue(fields()[4], other.strings)) {
                    this.strings = data().deepCopy(fields()[4].schema(), other.strings);
                    fieldSetFlags()[4] = true;
                }
                if (isValidValue(fields()[5], other.longs)) {
                    this.longs = data().deepCopy(fields()[5].schema(), other.longs);
                    fieldSetFlags()[5] = true;
                }
            }

            /** Gets the value of the 'database' field */
            public CharSequence getDatabase() {
                return database;
            }

            /** Sets the value of the 'database' field */
            public Builder setDatabase(CharSequence value) {
                validate(fields()[0], value);
                this.database = value;
                fieldSetFlags()[0] = true;
                return this;
            }

            /** Checks whether the 'database' field has been set */
            public boolean hasDatabase() {
                return fieldSetFlags()[0];
            }

            /** Clears the value of the 'database' field */
            public Builder clearDatabase() {
                database = null;
                fieldSetFlags()[0] = false;
                return this;
            }

            /** Gets the value of the 'table' field */
            public CharSequence getTable() {
                return table;
            }

            /** Sets the value of the 'table' field */
            public Builder setTable(CharSequence value) {
                validate(fields()[1], value);
                this.table = value;
                fieldSetFlags()[1] = true;
                return this;
            }

            /** Checks whether the 'table' field has been set */
            public boolean hasTable() {
                return fieldSetFlags()[1];
            }

            /** Clears the value of the 'table' field */
            public Builder clearTable() {
                table = null;
                fieldSetFlags()[1] = false;
                return this;
            }

            /** Gets the value of the 'tableId' field */
            public Long getTableId() {
                return tableId;
            }

            /** Sets the value of the 'tableId' field */
            public Builder setTableId(long value) {
                validate(fields()[2], value);
                this.tableId = value;
                fieldSetFlags()[2] = true;
                return this;
            }

            /** Checks whether the 'tableId' field has been set */
            public boolean hasTableId() {
                return fieldSetFlags()[2];
            }

            /** Clears the value of the 'tableId' field */
            public Builder clearTableId() {
                fieldSetFlags()[2] = false;
                return this;
            }

            /** Gets the value of the 'integers' field */
            public Map<CharSequence,Integer> getIntegers() {
                return integers;
            }

            /** Sets the value of the 'integers' field */
            public Builder setIntegers(Map<CharSequence,Integer> value) {
                validate(fields()[3], value);
                this.integers = value;
                fieldSetFlags()[3] = true;
                return this;
            }

            /** Checks whether the 'integers' field has been set */
            public boolean hasIntegers() {
                return fieldSetFlags()[3];
            }

            /** Clears the value of the 'integers' field */
            public Builder clearIntegers() {
                integers = null;
                fieldSetFlags()[3] = false;
                return this;
            }

            /** Gets the value of the 'strings' field */
            public Map<CharSequence,CharSequence> getStrings() {
                return strings;
            }

            /** Sets the value of the 'strings' field */
            public Builder setStrings(Map<CharSequence,CharSequence> value) {
                validate(fields()[4], value);
                this.strings = value;
                fieldSetFlags()[4] = true;
                return this;
            }

            /** Checks whether the 'strings' field has been set */
            public boolean hasStrings() {
                return fieldSetFlags()[4];
            }

            /** Clears the value of the 'strings' field */
            public Builder clearStrings() {
                strings = null;
                fieldSetFlags()[4] = false;
                return this;
            }

            /** Gets the value of the 'longs' field */
            public Map<CharSequence,Long> getLongs() {
                return longs;
            }

            /** Sets the value of the 'longs' field */
            public Builder setLongs(Map<CharSequence,Long> value) {
                validate(fields()[5], value);
                this.longs = value;
                fieldSetFlags()[5] = true;
                return this;
            }

            /** Checks whether the 'longs' field has been set */
            public boolean hasLongs() {
                return fieldSetFlags()[5];
            }

            /** Clears the value of the 'longs' field */
            public Builder clearLongs() {
                longs = null;
                fieldSetFlags()[5] = false;
                return this;
            }

            @Override
            public InsertMutation build() {
                try {
                    InsertMutation record = new InsertMutation();
                    record.database = fieldSetFlags()[0] ? this.database : (CharSequence) defaultValue(fields()[0]);
                    record.table = fieldSetFlags()[1] ? this.table : (CharSequence) defaultValue(fields()[1]);
                    record.tableId = fieldSetFlags()[2] ? this.tableId : (Long) defaultValue(fields()[2]);
                    record.integers = fieldSetFlags()[3] ? this.integers : (Map<CharSequence,Integer>) defaultValue(fields()[3]);
                    record.strings = fieldSetFlags()[4] ? this.strings : (Map<CharSequence,CharSequence>) defaultValue(fields()[4]);
                    record.longs = fieldSetFlags()[5] ? this.longs : (Map<CharSequence,Long>) defaultValue(fields()[5]);
                    return record;
                } catch (Exception e) {
                    throw new org.apache.avro.AvroRuntimeException(e);
                }
            }
        }
    }
}
