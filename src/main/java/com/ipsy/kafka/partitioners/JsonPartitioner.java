package com.ipsy.kafka.partitioners;

import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import io.confluent.connect.storage.common.StorageCommonConfig;


public class JsonPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(JsonPartitioner.class);
    private List<String> fieldNames;
    private static final String fieldDelim = Pattern.quote(".");
    private static final String defaultEncodedPartitioner = "unrecognized";


    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        java.util.HashMap hs = (java.util.HashMap)value;

        try {
            if (value instanceof HashMap) {
                final HashMap map = (HashMap) value;

                StringBuilder builder = new StringBuilder();
                for (String fieldName : fieldNames) {
                    log.debug("fieldName: "+fieldName);
                    HashMap intermediate = map;

                    if (builder.length() > 0) {
                        builder.append(this.delim);
                    }

                    for (String fieldPart : fieldName.trim().split(fieldDelim)) {
                        log.debug("fieldPart: "+fieldPart);

                        // Check if value has more childs
                        Object partitionKey = intermediate.get(fieldPart);
                        if (partitionKey instanceof HashMap) {
                            log.debug("PartitionKey is HashMap type.");
                            intermediate = (HashMap) partitionKey;
                        } else {
                            log.debug("PartitionKey is not HashMap type.");
                            builder.append((String) partitionKey);
                        }
                    }
                }
                if (builder.length() > 0) {
                    log.debug("We could not find a valid field name, using default");
                    return defaultEncodedPartitioner;
                }

                String encodedPartition = builder.toString();
                log.debug("Final encoded partition is "+encodedPartition);
                return builder.toString();
            } else {
                log.error("Value is not HashMap type.");
                return defaultEncodedPartitioner;
            }
        } catch (Exception ex) {
            log.error("Unhandled exception.");
            log.error(ex.getMessage());
            log.error(ex.getStackTrace().toString());
            return defaultEncodedPartitioner;
        }
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                    Utils.join(fieldNames, ",")
            );
        }
        return partitionFields;
    }
}
