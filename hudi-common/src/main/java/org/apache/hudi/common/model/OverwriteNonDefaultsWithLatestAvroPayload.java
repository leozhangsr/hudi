/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class OverwriteNonDefaultsWithLatestAvroPayload extends OverwriteWithLatestAvroPayload {

  public OverwriteNonDefaultsWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OverwriteNonDefaultsWithLatestAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    return preCombine(oldValue, null, null);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue,
                                                   Properties properties, Schema schema) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    int compare = oldValue.orderingVal.compareTo(orderingVal);
    if (compare > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else if (compare < 0) {
      return this;
    }
    if (schema != null) {
      try {
        Option<IndexedRecord> newRecord = getInsertValue(schema);
        Option<IndexedRecord> oldRecord = oldValue.getInsertValue(schema);
        if (newRecord.isPresent() && !oldRecord.isPresent()) {
          return this;
        }
        if (!newRecord.isPresent() && oldRecord.isPresent()) {
          return oldValue;
        }
        if (!newRecord.isPresent() && !oldRecord.isPresent()) {
          return this;
        }
        GenericRecord newIndexedRecord = (GenericRecord) newRecord.get();
        GenericRecord oldIndexedRecord = (GenericRecord) oldRecord.get();
        List<Schema.Field> fields = schema.getFields();
        fields.forEach(field -> {
          Object value = newIndexedRecord.get(field.name());
          value = field.schema().getType().equals(Schema.Type.STRING) && value != null
              ? value.toString() : value;
          Object defaultValue = field.defaultVal();
          if (!overwriteField(value, defaultValue)) {
            oldIndexedRecord.put(field.name(), value);
          }
        });
        return new OverwriteNonDefaultsWithLatestAvroPayload(oldIndexedRecord, this.orderingVal);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;

    if (isDeleteRecord(insertRecord)) {
      return Option.empty();
    } else {
      List<Schema.Field> fields = schema.getFields();
      fields.forEach(field -> {
        Object value = insertRecord.get(field.name());
        value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
        Object defaultValue = field.defaultVal();
        if (!overwriteField(value, defaultValue)) {
          currentRecord.put(field.name(), value);
        }
      });
      return Option.of(currentRecord);
    }
  }
}
