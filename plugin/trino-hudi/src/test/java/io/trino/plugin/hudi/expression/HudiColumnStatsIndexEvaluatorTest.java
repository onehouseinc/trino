/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.expression;

import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class HudiColumnStatsIndexEvaluatorTest
{
    private HudiColumnStatsIndexEvaluator evaluator;

    /**
     * A concrete implementation of {@code HudiTrinoFunctionExpression} for testing purposes, removing the need for mocks.
     */
    private static class TestHudiTrinoFunctionExpression
            extends HudiTrinoFunctionExpression
    {
        private final org.apache.hudi.internal.schema.Type hudiType;

        TestHudiTrinoFunctionExpression(org.apache.hudi.internal.schema.Type hudiType)
        {
            // Empty list of arguments used
            super(Collections.emptyList(), hudiType);
            this.hudiType = hudiType;
        }

        @Override
        public Object castToTrinoType(Object value)
        {
            // The evaluator calls this on min/max values from stats
            // In the tests, these values are already the correct Java types, so just return them
            return value;
        }

        @Override
        public org.apache.hudi.internal.schema.Type getDataType()
        {
            return hudiType;
        }
    }

    @BeforeEach
    void setUp()
    {
        evaluator = new HudiColumnStatsIndexEvaluator();
    }

    private HoodieMetadataColumnStats createStats(Object min, Object max, long nullCount, long valueCount)
    {
        // Create a schema that can hold any of the types we test with
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"HoodieMetadataRecord\",\"namespace\":\"org.apache.hudi.avro.model\",\"doc\":\"A record saved within the Metadata Table\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"type\",\"type\":\"int\",\"doc\":\"Type of the metadata record\"},{\"name\":\"filesystemMetadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"HoodieMetadataFileInfo\",\"fields\":[{\"name\":\"size\",\"type\":\"long\",\"doc\":\"Size of the file\"},{\"name\":\"isDeleted\",\"type\":\"boolean\",\"doc\":\"True if this file has been deleted\"}]},\"avro.java.string\":\"String\"}],\"doc\":\"Contains information about partitions and files within the dataset\"},{\"name\":\"BloomFilterMetadata\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HoodieMetadataBloomFilter\",\"doc\":\"Data file bloom filter details\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Bloom filter type code\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Instant timestamp when this metadata was created/updated\"},{\"name\":\"bloomFilter\",\"type\":\"bytes\",\"doc\":\"Bloom filter binary byte array\"},{\"name\":\"isDeleted\",\"type\":\"boolean\",\"doc\":\"Bloom filter entry valid/deleted flag\"}]}],\"doc\":\"Metadata Index of bloom filters for all data files in the user table\",\"default\":null},{\"name\":\"ColumnStatsMetadata\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HoodieMetadataColumnStats\",\"doc\":\"Data file column statistics\",\"fields\":[{\"name\":\"fileName\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"File name for which this column statistics applies\",\"default\":null},{\"name\":\"columnName\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Column name for which this column statistics applies\",\"default\":null},{\"name\":\"minValue\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BooleanWrapper\",\"doc\":\"A record wrapping boolean type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"boolean\"}]},{\"type\":\"record\",\"name\":\"IntWrapper\",\"doc\":\"A record wrapping int type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]},{\"type\":\"record\",\"name\":\"LongWrapper\",\"doc\":\"A record wrapping long type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"FloatWrapper\",\"doc\":\"A record wrapping float type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"float\"}]},{\"type\":\"record\",\"name\":\"DoubleWrapper\",\"doc\":\"A record wrapping double type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"double\"}]},{\"type\":\"record\",\"name\":\"BytesWrapper\",\"doc\":\"A record wrapping bytes type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"bytes\"}]},{\"type\":\"record\",\"name\":\"StringWrapper\",\"doc\":\"A record wrapping string type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]},{\"type\":\"record\",\"name\":\"DateWrapper\",\"doc\":\"A record wrapping Date logical type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]},{\"type\":\"record\",\"name\":\"DecimalWrapper\",\"doc\":\"A record wrapping Decimal logical type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":30,\"scale\":15}}]},{\"type\":\"record\",\"name\":\"TimeMicrosWrapper\",\"doc\":\"A record wrapping Time-micros logical type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}]},{\"type\":\"record\",\"name\":\"TimestampMicrosWrapper\",\"doc\":\"A record wrapping Timestamp-micros logical type to be able to be used it w/in Avro's Union\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}],\"doc\":\"Minimum value in the range. Based on user data table schema, we can convert this to appropriate type\",\"default\":null},{\"name\":\"maxValue\",\"type\":[\"null\",\"BooleanWrapper\",\"IntWrapper\",\"LongWrapper\",\"FloatWrapper\",\"DoubleWrapper\",\"BytesWrapper\",\"StringWrapper\",\"DateWrapper\",\"DecimalWrapper\",\"TimeMicrosWrapper\",\"TimestampMicrosWrapper\"],\"doc\":\"Maximum value in the range. Based on user data table schema, we can convert it to appropriate type\",\"default\":null},{\"name\":\"valueCount\",\"type\":[\"null\",\"long\"],\"doc\":\"Total count of values\",\"default\":null},{\"name\":\"nullCount\",\"type\":[\"null\",\"long\"],\"doc\":\"Total count of null values\",\"default\":null},{\"name\":\"totalSize\",\"type\":[\"null\",\"long\"],\"doc\":\"Total storage size on disk\",\"default\":null},{\"name\":\"totalUncompressedSize\",\"type\":[\"null\",\"long\"],\"doc\":\"Total uncompressed storage size on disk\",\"default\":null},{\"name\":\"isDeleted\",\"type\":\"boolean\",\"doc\":\"Column range entry valid/deleted flag\"}]}],\"doc\":\"Metadata Index of column statistics for all data files in the user table\",\"default\":null},{\"name\":\"recordIndexMetadata\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HoodieRecordIndexInfo\",\"fields\":[{\"name\":\"partitionName\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Refers to the partition name the record belongs to\",\"default\":null},{\"name\":\"fileIdHighBits\",\"type\":[\"null\",\"long\"],\"doc\":\"Refers to high 64 bits if the fileId is based on UUID format. \\nA UUID based fileId is stored as 3 pieces in RLI (fileIdHighBits, fileIdLowBits and fileIndex). \\nFileID format is {UUID}-{fileIndex}.\",\"default\":null},{\"name\":\"fileIdLowBits\",\"type\":[\"null\",\"long\"],\"doc\":\"Refers to low 64 bits if the fileId is based on UUID format. \\nA UUID based fileId is stored as 3 pieces in RLI (fileIdHighBits, fileIdLowBits and fileIndex). \\nFileID format is {UUID}-{fileIndex}.\",\"default\":null},{\"name\":\"fileIndex\",\"type\":[\"null\",\"int\"],\"doc\":\"Index representing file index which is used to re-construct UUID based fileID. Applicable when the fileId is based on UUID format. \\nA UUID based fileId is stored as 3 pieces in RLI (fileIdHighBits, fileIdLowBits and fileIndex). \\nFileID format is {UUID}-{fileIndex}.\",\"default\":null},{\"name\":\"fileId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Represents fileId of the location where record belongs to. When the encoding is 1, fileID is stored in raw string format.\",\"default\":null},{\"name\":\"instantTime\",\"type\":[\"null\",\"long\"],\"doc\":\"Epoch time in millisecond representing the commit time at which record was added\",\"default\":null},{\"name\":\"fileIdEncoding\",\"type\":\"int\",\"doc\":\"Represents fileId encoding. Possible values are 0 and 1. O represents UUID based fileID, and 1 represents raw string format of the fileId. \\nWhen the encoding is 0, reader can deduce fileID from fileIdLowBits, fileIdLowBits and fileIndex.\",\"default\":0}]}],\"doc\":\"Metadata Index that contains information about record keys and their location in the dataset\",\"default\":null}]}");
        GenericRecord minRecord = new GenericData.Record(avroSchema);
        minRecord.put(0, min);
        GenericRecord maxRecord = new GenericData.Record(avroSchema);
        maxRecord.put(0, max);

        return HoodieMetadataColumnStats.newBuilder()
                .setMinValue(minRecord)
                .setMaxValue(maxRecord)
                .setNullCount(nullCount)
                .setValueCount(valueCount)
                .setFileName("file1.parquet")
                .setColumnName("test_col")
                .setIsDeleted(false)
                .build();
    }

    private static Stream<Arguments> binaryComparisonPruningTests()
    {
        // Arguments: description, min, max, operator, literalValue, trinoType, expectedResult, isFlipped
        return Stream.of(
                // --- Integer Tests (isFlipped = false; literal on RHS) ---
                Arguments.of("EQ: within range", 10, 30, Expression.Operator.EQ, 20, IntegerType.INTEGER, true, false),
                Arguments.of("EQ: below range", 10, 30, Expression.Operator.EQ, 5, IntegerType.INTEGER, false, false),
                Arguments.of("EQ: above range", 10, 30, Expression.Operator.EQ, 35, IntegerType.INTEGER, false, false),
                Arguments.of("EQ: on min boundary", 10, 30, Expression.Operator.EQ, 10, IntegerType.INTEGER, true, false),
                Arguments.of("EQ: on max boundary", 10, 30, Expression.Operator.EQ, 30, IntegerType.INTEGER, true, false),

                Arguments.of("LT: prune case", 10, 30, Expression.Operator.LT, 10, IntegerType.INTEGER, false, false),
                Arguments.of("LT: scan case", 10, 30, Expression.Operator.LT, 11, IntegerType.INTEGER, true, false),
                Arguments.of("LT: scan case wide", 10, 30, Expression.Operator.LT, 40, IntegerType.INTEGER, true, false),

                Arguments.of("LT_EQ: prune case", 10, 30, Expression.Operator.LT_EQ, 9, IntegerType.INTEGER, false, false),
                Arguments.of("LT_EQ: scan case", 10, 30, Expression.Operator.LT_EQ, 10, IntegerType.INTEGER, true, false),

                Arguments.of("GT: prune case", 10, 30, Expression.Operator.GT, 30, IntegerType.INTEGER, false, false),
                Arguments.of("GT: scan case", 10, 30, Expression.Operator.GT, 29, IntegerType.INTEGER, true, false),
                Arguments.of("GT: scan case wide", 10, 30, Expression.Operator.GT, 5, IntegerType.INTEGER, true, false),

                Arguments.of("GT_EQ: prune case", 10, 30, Expression.Operator.GT_EQ, 31, IntegerType.INTEGER, false, false),
                Arguments.of("GT_EQ: scan case", 10, 30, Expression.Operator.GT_EQ, 30, IntegerType.INTEGER, true, false),

                // --- String Tests (isFlipped = false; literal on RHS) ---
                Arguments.of("EQ: string below range", "b", "d", Expression.Operator.EQ, "a", VarcharType.createVarcharType(10), false, false),
                Arguments.of("EQ: string above range", "b", "d", Expression.Operator.EQ, "e", VarcharType.createVarcharType(10), false, false),
                Arguments.of("EQ: string within range", "b", "d", Expression.Operator.EQ, "c", VarcharType.createVarcharType(10), true, false),

                Arguments.of("LT: string prune case", "b", "d", Expression.Operator.LT, "b", VarcharType.createVarcharType(10), false, false),
                Arguments.of("LT: string scan case", "b", "d", Expression.Operator.LT, "c", VarcharType.createVarcharType(10), true, false),

                // --- Flipped Operand Tests (isFlipped = true; literal on LHS) ---
                Arguments.of("Flipped GT (LT): scan case", 10, 30, Expression.Operator.GT, 31, IntegerType.INTEGER, true, true),
                Arguments.of("Flipped LT (GT)]: scan case", 10, 30, Expression.Operator.LT, 9, IntegerType.INTEGER, true, true));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("binaryComparisonPruningTests")
    void testBinaryComparisonPruning(String description, Object min, Object max, Expression.Operator op, Object literalValue, Type trinoType, boolean expected, boolean isFlipped)
    {
        evaluator.setStats(createStats(min, max, 0, 100));
        org.apache.hudi.internal.schema.Type hudiType = HudiTypeConverter.getType(trinoType);
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);
        Literal<?> literal = new Literal<>(literalValue, hudiType);

        Predicates.BinaryComparison comparison = isFlipped
                ? new Predicates.BinaryComparison(literal, op, columnReference)
                : new Predicates.BinaryComparison(columnReference, op, literal);

        assertThat(comparison.accept(evaluator))
                .describedAs(description)
                .isEqualTo(expected);
    }

    @Test
    void testAndPruning()
    {
        evaluator.setStats(createStats(10, 20, 0, 100));
        org.apache.hudi.internal.schema.Type hudiType = org.apache.hudi.internal.schema.Types.IntType.get();
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);

        // col > 5 AND col < 15. Stats: min=10, max=20 -> scan
        Predicates.BinaryComparison left = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(5, hudiType));
        Predicates.BinaryComparison right = new Predicates.BinaryComparison(columnReference, Expression.Operator.LT, new Literal<>(15, hudiType));
        Predicates.And and = new Predicates.And(left, right);
        assertThat(and.accept(evaluator)).isTrue();

        // col > 25 AND col < 30. Stats: min=10, max=20 -> prune
        left = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(25, hudiType));
        right = new Predicates.BinaryComparison(columnReference, Expression.Operator.LT, new Literal<>(30, hudiType));
        and = new Predicates.And(left, right);
        assertThat(and.accept(evaluator)).isFalse();
    }

    @Test
    void testOrPruning()
    {
        evaluator.setStats(createStats(10, 20, 0, 100));
        org.apache.hudi.internal.schema.Type hudiType = org.apache.hudi.internal.schema.Types.IntType.get();
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);

        // col < 5 OR col > 15. Stats: min=10, max=20 -> scan
        Predicates.BinaryComparison left = new Predicates.BinaryComparison(columnReference, Expression.Operator.LT, new Literal<>(5, hudiType));
        Predicates.BinaryComparison right = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(15, hudiType));
        Predicates.Or or = new Predicates.Or(left, right);
        assertThat(or.accept(evaluator)).isTrue();

        // col < 5 OR col > 25. Stats: min=10, max=20 -> prune
        right = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(25, hudiType));
        or = new Predicates.Or(left, right);
        assertThat(or.accept(evaluator)).isFalse();
    }

    @Test
    void testNotOperator()
    {
        evaluator.setStats(createStats(10, 20, 0, 100));
        org.apache.hudi.internal.schema.Type hudiType = org.apache.hudi.internal.schema.Types.IntType.get();
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);

        // NOT (col > 25) -> col <= 25. Stats: min=10, max=20 -> scan
        Predicates.BinaryComparison inner = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(25, hudiType));
        Predicates.Not not = new Predicates.Not(inner);
        assertThat(not.accept(evaluator)).isTrue();

        // NOT (col > 5) -> col <= 5. Stats: min=10, max=20 -> prune
        inner = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(5, hudiType));
        not = new Predicates.Not(inner);
        assertThat(not.accept(evaluator)).isFalse();
    }

    @Test
    void testNotEqOperator()
    {
        org.apache.hudi.internal.schema.Type hudiType = org.apache.hudi.internal.schema.Types.IntType.get();
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);

        // NOT (col = 25). Stats: min=10, max=20. EQ is false, so NOT EQ is true. Stats: min=10, max=20 -> scan
        evaluator.setStats(createStats(10, 20, 0, 100));
        Predicates.BinaryComparison inner = new Predicates.BinaryComparison(columnReference, Expression.Operator.EQ, new Literal<>(25, hudiType));
        Predicates.Not not = new Predicates.Not(inner);
        assertThat(not.accept(evaluator)).isTrue();

        // NOT (col = 15). Stats: min=15, max=15, no nulls. EQ is true, so NOT EQ is false. Stats: min=15, max=15 -> prune
        evaluator.setStats(createStats(15, 15, 0, 100));
        inner = new Predicates.BinaryComparison(columnReference, Expression.Operator.EQ, new Literal<>(15, hudiType));
        not = new Predicates.Not(inner);
        assertThat(not.accept(evaluator)).isFalse();
    }

    @Test
    void testAllNullsInStats()
    {
        evaluator.setStats(createStats(null, null, 100, 100));
        org.apache.hudi.internal.schema.Type hudiType = org.apache.hudi.internal.schema.Types.IntType.get();
        HudiTrinoFunctionExpression columnReference = new TestHudiTrinoFunctionExpression(hudiType);

        // col > 15 against all-null column. Stats: min=null, max=null -> prune
        Predicates.BinaryComparison comparison = new Predicates.BinaryComparison(columnReference, Expression.Operator.GT, new Literal<>(15, hudiType));
        assertThat(comparison.accept(evaluator)).isFalse();

        // col = NULL (IS NULL). All nulls exist. Stats: min=null, max=null -> scan
        comparison = new Predicates.BinaryComparison(columnReference, Expression.Operator.EQ, new Literal<>(null, hudiType));
        assertThat(comparison.accept(evaluator)).isTrue();
    }

    @Test
    void testUnsupportedOperations()
    {
        evaluator.setStats(createStats(10, 20, 0, 100));
        assertThatThrownBy(() -> evaluator.visitFunction(null))
                .isInstanceOf(UnsupportedOperationException.class);

        // Create a dummy expression to pass to the CastExpression constructor
        Literal<Integer> dummyChild = new Literal<>(1, org.apache.hudi.internal.schema.Types.IntType.get());
        CastExpression cast = new CastExpression(dummyChild, org.apache.hudi.internal.schema.Types.LongType.get());
        assertThatThrownBy(() -> evaluator.visitCast(cast))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testAlwaysAndFalse()
    {
        assertThat(evaluator.alwaysTrue()).isTrue();
        assertThat(evaluator.alwaysFalse()).isFalse();
    }
}
