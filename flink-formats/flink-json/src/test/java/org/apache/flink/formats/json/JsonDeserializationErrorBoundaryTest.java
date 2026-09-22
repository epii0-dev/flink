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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.testutils.formats.SchemaTestUtils.open;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.DEBUG;

/**
 * Regression tests for the error boundary between JSON parsing/conversion and downstream {@link
 * Collector} failures in {@link JsonRowDataDeserializationSchema} and {@link
 * JsonParserRowDataDeserializationSchema}.
 *
 * <p>Expected contract:
 *
 * <ul>
 *   <li>Exceptions thrown by {@link Collector#collect} are downstream failures, not parse errors.
 *       They must propagate unchanged and must never be swallowed by {@code ignore-parse-errors}.
 *   <li>Diagnostics for genuine parse failures must be bounded and must not embed the full raw
 *       input in the exception message, in the messages of causes/suppressed exceptions, in the
 *       stringified stack trace, in the serialized throwable, or in the debug log.
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class JsonDeserializationErrorBoundaryTest {

    private static final String TAIL_MARKER = "TAIL-MARKER-4d61f9c3";

    private static final int MAX_DIAGNOSTIC_LENGTH = 4096;

    private static final int MAX_SERIALIZED_THROWABLE_LENGTH = 64 * 1024;

    private static final RowType ROW_TYPE =
            (RowType) ROW(FIELD("id", INT()), FIELD("name", STRING())).getLogicalType();

    @RegisterExtension
    public final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(AbstractJsonDeserializationSchema.class, DEBUG);

    @Parameter public boolean isJsonParser;

    @Parameters(name = "isJsonParser={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    /**
     * A valid JSON object whose {@link Collector} fails. The downstream exception must propagate
     * unchanged; it is not a parse error and must not be wrapped as "Failed to deserialize JSON".
     */
    @TestTemplate
    void testCollectorExceptionPropagatesFromObject() {
        DeserializationSchema<RowData> schema = createSchema(false);
        RuntimeException downstream = new RuntimeException("simulated downstream failure");

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        "{\"id\":1,\"name\":\"a\"}"
                                                .getBytes(StandardCharsets.UTF_8),
                                        failingCollector(downstream)))
                .isSameAs(downstream);
    }

    /**
     * With {@code ignore-parse-errors=true}, a {@link Collector} failure on a valid JSON object
     * must still propagate. It is not a parse error and must not be silently swallowed.
     */
    @TestTemplate
    void testCollectorExceptionPropagatesFromObjectIgnoreParseErrors() {
        DeserializationSchema<RowData> schema = createSchema(true);
        RuntimeException downstream = new RuntimeException("simulated downstream failure");

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        "{\"id\":1,\"name\":\"a\"}"
                                                .getBytes(StandardCharsets.UTF_8),
                                        failingCollector(downstream)))
                .isSameAs(downstream);
    }

    /**
     * A valid JSON array whose {@link Collector} fails on the second element. The downstream
     * exception must propagate unchanged and no element after the failure may be emitted.
     */
    @TestTemplate
    void testCollectorExceptionPropagatesFromArray() {
        DeserializationSchema<RowData> schema = createSchema(false);
        RuntimeException downstream = new RuntimeException("simulated downstream failure");
        byte[] message =
                "[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"},{\"id\":3,\"name\":\"c\"}]"
                        .getBytes(StandardCharsets.UTF_8);
        List<RowData> collected = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        message, failingCollectorAt(downstream, collected, 2)))
                .isSameAs(downstream);
        assertThat(collected).hasSize(1);
    }

    /**
     * With {@code ignore-parse-errors=true}, a {@link Collector} failure in the middle of a JSON
     * array must propagate. Swallowing it hides the downstream failure and either drops or silently
     * continues emitting the remaining elements.
     */
    @TestTemplate
    void testCollectorExceptionPropagatesFromArrayIgnoreParseErrors() {
        DeserializationSchema<RowData> schema = createSchema(true);
        RuntimeException downstream = new RuntimeException("simulated downstream failure");
        byte[] message =
                "[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"},{\"id\":3,\"name\":\"c\"}]"
                        .getBytes(StandardCharsets.UTF_8);
        List<RowData> collected = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        message, failingCollectorAt(downstream, collected, 2)))
                .isSameAs(downstream);
        assertThat(collected).hasSize(1);
    }

    /**
     * Genuine parse failures keep the existing semantics: {@code ignore-parse-errors=false} fails
     * with an {@link IOException}; {@code ignore-parse-errors=true} skips the record.
     */
    @TestTemplate
    void testGenuineParseErrorsKeepExistingSemantics() throws Exception {
        byte[] corrupt = "not valid json".getBytes(StandardCharsets.UTF_8);

        DeserializationSchema<RowData> failingSchema = createSchema(false);
        assertThatThrownBy(
                        () ->
                                failingSchema.deserialize(
                                        corrupt, new ListCollector<>(new ArrayList<>())))
                .isInstanceOf(IOException.class);

        DeserializationSchema<RowData> ignoringSchema = createSchema(true);
        List<RowData> collected = new ArrayList<>();
        ignoringSchema.deserialize(corrupt, new ListCollector<>(collected));
        assertThat(collected).isEmpty();
    }

    /**
     * The exception raised for a genuine parse failure must carry bounded diagnostics: neither the
     * outer message, nor any cause/suppressed message, nor the stringified stack trace, nor the
     * serialized throwable may embed the full raw input.
     */
    @TestTemplate
    void testParseErrorDoesNotEmbedFullInput() {
        DeserializationSchema<RowData> schema = createSchema(false);
        byte[] corrupt = largeCorruptMessage();

        assertThatThrownBy(
                        () -> schema.deserialize(corrupt, new ListCollector<>(new ArrayList<>())))
                .isInstanceOf(IOException.class)
                .satisfies(
                        t -> {
                            // record the real diagnostic sizes in the surefire output before
                            // any bound assertion runs
                            System.err.printf(
                                    "DIAG-SIZE isJsonParser=%s serializedBytes=%d stringifiedChars=%d%n",
                                    isJsonParser,
                                    serialize(t).length,
                                    ExceptionUtils.stringifyException(t).length());
                            assertBoundedThrowableMessages(t);
                            assertThat(ExceptionUtils.stringifyException(t))
                                    .doesNotContain(TAIL_MARKER);
                            assertThat(serialize(t).length)
                                    .isLessThan(MAX_SERIALIZED_THROWABLE_LENGTH);
                        });
    }

    /**
     * With {@code ignore-parse-errors=true} and DEBUG logging enabled, the diagnostic log event for
     * a genuine parse failure must exist and must not embed the full raw input in either its
     * formatted message or its attached throwable.
     */
    @TestTemplate
    void testParseErrorDebugLogDoesNotEmbedFullInput() throws Exception {
        DeserializationSchema<RowData> schema = createSchema(true);
        byte[] corrupt = largeCorruptMessage();

        List<RowData> collected = new ArrayList<>();
        schema.deserialize(corrupt, new ListCollector<>(collected));
        assertThat(collected).isEmpty();

        List<LogEvent> events = loggerExtension.getEvents();
        assertThat(events)
                .as("expected at least one parse-failure debug log event to be captured")
                .isNotEmpty();
        for (LogEvent event : events) {
            assertThat(event.getMessage().getFormattedMessage()).doesNotContain(TAIL_MARKER);
            assertThat(event.getMessage().getFormattedMessage().length())
                    .isLessThan(MAX_DIAGNOSTIC_LENGTH);
            if (event.getThrown() != null) {
                assertBoundedThrowableMessages(event.getThrown());
            }
        }
    }

    /**
     * An invalid ~1 MiB document carrying a marker at its tail. The leading {@code '!'} is an
     * unambiguously invalid top-level token, so both deserializers fail during root-level JSON
     * parsing before any row conversion.
     */
    private static byte[] largeCorruptMessage() {
        StringBuilder sb = new StringBuilder((1 << 20) + 64);
        sb.append('!');
        while (sb.length() < (1 << 20)) {
            sb.append("0123456789abcdef");
        }
        sb.append(TAIL_MARKER);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Traverses the cause chain and suppressed exceptions of {@code t} with cycle protection and
     * asserts that no message contains the raw-input marker or exceeds the diagnostic bound.
     */
    private static void assertBoundedThrowableMessages(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(t);
        while (!pending.isEmpty()) {
            Throwable current = pending.poll();
            if (!seen.add(current)) {
                continue;
            }
            String message = current.getMessage();
            if (message != null) {
                assertThat(message).doesNotContain(TAIL_MARKER);
                assertThat(message.length()).isLessThan(MAX_DIAGNOSTIC_LENGTH);
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
    }

    private static byte[] serialize(Throwable t) {
        try {
            return InstantiationUtil.serializeObject(t);
        } catch (IOException e) {
            throw new RuntimeException("failed to serialize throwable", e);
        }
    }

    private DeserializationSchema<RowData> createSchema(boolean ignoreParseErrors) {
        DeserializationSchema<RowData> schema =
                isJsonParser
                        ? new JsonParserRowDataDeserializationSchema(
                                ROW_TYPE,
                                InternalTypeInfo.of(ROW_TYPE),
                                false,
                                ignoreParseErrors,
                                TimestampFormat.SQL)
                        : new JsonRowDataDeserializationSchema(
                                ROW_TYPE,
                                InternalTypeInfo.of(ROW_TYPE),
                                false,
                                ignoreParseErrors,
                                TimestampFormat.SQL);
        open(schema);
        return schema;
    }

    private static Collector<RowData> failingCollector(RuntimeException failure) {
        return new Collector<RowData>() {
            @Override
            public void collect(RowData record) {
                throw failure;
            }

            @Override
            public void close() {}
        };
    }

    /**
     * A collector that forwards records to {@code collected} and throws {@code failure} on the
     * {@code failingCollect}-th call.
     */
    private static Collector<RowData> failingCollectorAt(
            RuntimeException failure, List<RowData> collected, int failingCollect) {
        return new Collector<RowData>() {
            private int count;

            @Override
            public void collect(RowData record) {
                if (++count >= failingCollect) {
                    throw failure;
                }
                collected.add(record);
            }

            @Override
            public void close() {}
        };
    }
}
