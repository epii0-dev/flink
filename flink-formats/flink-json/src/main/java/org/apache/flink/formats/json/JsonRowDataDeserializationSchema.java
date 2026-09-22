/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@Internal
public class JsonRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    public JsonRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
    }

    @Override
    public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
        if (message == null) {
            return;
        }
        final JsonNode root;
        try {
            root = deserializeToJsonNode(message);
        } catch (Throwable t) {
            handleParseError(message, t);
            return;
        }
        if (root != null && root.isArray()) {
            ArrayNode arrayNode = (ArrayNode) root;
            for (int i = 0; i < arrayNode.size(); i++) {
                final RowData result;
                try {
                    result = convertToRowData(arrayNode.get(i));
                } catch (Throwable t) {
                    handleParseError(message, t);
                    continue;
                }
                if (result != null) {
                    // downstream Collector failures are not parse errors and propagate unchanged
                    out.collect(result);
                }
            }
        } else {
            final RowData result;
            try {
                result = convertToRowData(root);
            } catch (Throwable t) {
                handleParseError(message, t);
                return;
            }
            if (result != null) {
                out.collect(result);
            }
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) runtimeConverter.convert(message);
    }
}
