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
package io.trino.plugin.hudi.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

public class HudiAvroSerializer
{
    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };

    private final SynthesizedColumnHandler synthesizedColumnHandler;

    private final List<HiveColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Schema schema;
    private final ZoneId zoneId;

    public HudiAvroSerializer(List<HiveColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(HiveColumnHandle::getType).toList();
        // Fetches projected schema
        this.schema = constructSchema(columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getName).toList(),
                columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getHiveType).toList(), false);
        this.synthesizedColumnHandler = null;
        this.zoneId = ZoneId.systemDefault();
    }

    public HudiAvroSerializer(List<HiveColumnHandle> columnHandles, SynthesizedColumnHandler synthesizedColumnHandler, ConnectorSession session)
    {
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(HiveColumnHandle::getType).toList();
        // Fetches projected schema
        this.schema = constructSchema(columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getName).toList(),
                columnHandles.stream().filter(ch -> !ch.isHidden()).map(HiveColumnHandle::getHiveType).toList(), false);
        this.synthesizedColumnHandler = synthesizedColumnHandler;
        TimeZoneKey sessionTimeZoneKey = session.getTimeZoneKey();
        this.zoneId = sessionTimeZoneKey.getZoneId();
    }

    public IndexedRecord serialize(SourcePage sourcePage, int position)
    {
        IndexedRecord record = new GenericData.Record(schema);
        for (int i = 0; i < columnTypes.size(); i++) {
            Object value = getValue(sourcePage, i, position);
            record.put(i, value);
        }
        return record;
    }

    public Object getValue(SourcePage sourcePage, int channel, int position)
    {
        return columnTypes.get(channel).getObjectValue(null, sourcePage.getBlock(channel), position);
    }

    public void buildRecordInPage(PageBuilder pageBuilder, IndexedRecord record,
            Map<Integer, String> partitionValueMap, boolean skipMetaColumns)
    {
        pageBuilder.declarePosition();
        int startChannel = skipMetaColumns ? HOODIE_META_COLUMNS.size() : 0;
        int blockSeq = 0;
        int nonPartitionChannel = startChannel;
        for (int channel = startChannel; channel < columnTypes.size() + partitionValueMap.size(); channel++, blockSeq++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(blockSeq);
            if (partitionValueMap.containsKey(channel)) {
                appendTo(VarcharType.VARCHAR, partitionValueMap.get(channel), output, zoneId);
            }
            else {
                appendTo(columnTypes.get(nonPartitionChannel), record.get(nonPartitionChannel), output, zoneId);
                nonPartitionChannel++;
            }
        }
    }

    public void buildRecordInPage(PageBuilder pageBuilder, IndexedRecord record)
    {
        pageBuilder.declarePosition();
        int blockSeq = 0;
        for (int channel = 0; channel < columnTypes.size(); channel++, blockSeq++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(blockSeq);
            HiveColumnHandle columnHandle = columnHandles.get(channel);
            if (synthesizedColumnHandler.isSynthesizedColumn(columnHandle)) {
                synthesizedColumnHandler.getColumnStrategy(columnHandle).appendToBlock(output, columnTypes.get(channel));
            }
            else {
                // Record may not be projected, get index from it
                int fieldPosInSchema = record.getSchema().getField(columnHandle.getName()).pos();
                appendTo(columnTypes.get(channel), record.get(fieldPosInSchema), output, zoneId);
            }
        }
    }

    public void buildRecordInPage(PageBuilder pageBuilder, SourcePage sourcePage, int position,
            Map<Integer, String> partitionValueMap, boolean skipMetaColumns)
    {
        pageBuilder.declarePosition();
        int startChannel = skipMetaColumns ? HOODIE_META_COLUMNS.size() : 0;
        int blockSeq = 0;
        int nonPartitionChannel = startChannel;
        for (int channel = startChannel; channel < columnTypes.size() + partitionValueMap.size(); channel++, blockSeq++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(blockSeq);
            if (partitionValueMap.containsKey(channel)) {
                appendTo(VarcharType.VARCHAR, partitionValueMap.get(channel), output, zoneId);
            }
            else {
                appendTo(columnTypes.get(nonPartitionChannel), getValue(sourcePage, nonPartitionChannel, position), output, zoneId);
                nonPartitionChannel++;
            }
        }
    }

    public static void appendTo(Type type, Object value, BlockBuilder output, ZoneId zoneId)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(SMALLINT)) {
                    type.writeLong(output, ((Number) value).shortValue());
                }
                else if (type.equals(TINYINT)) {
                    type.writeLong(output, ((Number) value).byteValue());
                }
                else if (type.equals(REAL)) {
                    if (value instanceof Number) {
                        // Directly get the float value from the Number
                        // This preserves the fractional part
                        float floatValue = ((Number) value).floatValue();

                        // Get the IEEE 754 single-precision 32-bit representation of this float
                        int intBits = Float.floatToRawIntBits(floatValue);

                        // The writeLong method expects these int bits, passed as a long
                        // NOTE: Java handles the widening conversion from int to long
                        type.writeLong(output, intBits);
                    }
                    else {
                        // Handle cases where 'value' is not a Number
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Unhandled value type for REAL: %s type for %s: %s", value.getClass().getName(), javaType.getSimpleName(), type));
                    }
                }
                else if (type instanceof DecimalType decimalType) {
                    if (value instanceof SqlDecimal sqlDecimal) {
                        if (decimalType.isShort()) {
                            writeShortDecimal(output, sqlDecimal.toBigDecimal().unscaledValue().longValue());
                        }
                        else {
                            writeBigDecimal(decimalType, output, sqlDecimal.toBigDecimal());
                        }
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Cannot process value of type %s for DecimalType %s. Expected SqlDecimal",
                                        value.getClass().getName(),
                                        type.getTypeSignature()));
                    }
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, ((SqlDate) value).getDays());
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    type.writeLong(output, toTrinoTimestamp(((Utf8) value).toString()));
                }
                else if (type.equals(TIME_MICROS)) {
                    type.writeLong(output, (long) value * PICOSECONDS_PER_MICROSECOND);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (type.getJavaType() == Int128.class) {
                writeObject(output, type, value);
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == LongTimestamp.class) {
                if (value instanceof SqlTimestamp sqlTimestamp) {
                    // From tests, sqlTimestamp is a UTC epoch that is converted from ZoneId#systemDefault()
                    // IMPORTANT: Even when session's zoneId != ZoneId#systemDefault(), ZoneId#systemDefault() is used calculate/produce the false UTC.
                    // The current sqlTimestamp is calculated as such:
                    // 1. The true UTC timestamp that is stored in file is assumed to be in the local timezone
                    // 2. Trino will them attempt to convert this to a false UTC by subtracting the timezone's offset (factoring offset rules like DST)
                    // Hence, to calculate the true UTC, we will just have to reverse the steps

                    // Reconstruct the original local wall time from sqlTimestamp's fields
                    long microsFromSqlTs = sqlTimestamp.getEpochMicros();
                    // picosFromSqlTs is defined as "picoseconds within the microsecond" (0 to 999,999)
                    int picosFromSqlTs = sqlTimestamp.getPicosOfMicros();
                    long secondsComponent = microsFromSqlTs / 1_000_000L;
                    // Storing nanos component separately from seconds component, hence the modulo to remove secondsComponent
                    int nanosComponent = (int) ((microsFromSqlTs % 1_000_000L) * 1000L + picosFromSqlTs / 1000L);
                    LocalDateTime originalLocalWallTime = LocalDateTime.ofEpochSecond(secondsComponent, nanosComponent, ZoneOffset.UTC);

                    // Determine the ZoneId in which originalLocalWallTime was observed
                    ZoneId assumedOriginalZoneId = ZoneId.systemDefault();

                    // Convert to true UTC by interpreting the local wall time in its original zone
                    // This correctly handles DST for that zone at that specific historical date/time.
                    ZonedDateTime zdtInOriginalZone;
                    try {
                        zdtInOriginalZone = originalLocalWallTime.atZone(assumedOriginalZoneId);
                    }
                    catch (DateTimeException e) {
                        // Handle cases where the local time is invalid in the zone (e.g., during DST "spring forward" gap) or ambiguous (during DST "fall back" overlap).
                        // For now, rethrow or log, as robustly handling this requires a defined policy.
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Cannot uniquely or validly map local time %s to zone %s: %s",
                                        originalLocalWallTime, assumedOriginalZoneId, e.getMessage()), e);
                    }
                    Instant trueUtcInstant = zdtInOriginalZone.toInstant();

                    // Extract true UTC epoch micros and picos
                    long trueUtcEpochMicros = trueUtcInstant.getEpochSecond() * 1_000_000L + trueUtcInstant.getNano() / 1000L;
                    int truePicosOfMicros = (trueUtcInstant.getNano() % 1000) * 1000;

                    ((Fixed12BlockBuilder) output).writeFixed12(trueUtcEpochMicros, truePicosOfMicros);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                verify(type.equals(TIMESTAMP_TZ_MICROS));
                long epochMicros = (long) value;
                int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
                type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
            }
            else if (type instanceof ArrayType arrayType) {
                writeArray((ArrayBlockBuilder) output, (List<?>) value, arrayType, zoneId);
            }
            else if (type instanceof RowType rowType) {
                // value is usually an instance of UnmodifiableRandomAccessList
                if (value instanceof List list) {
                    writeRow((RowBlockBuilder) output, rowType, list, zoneId);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (type instanceof MapType mapType) {
                writeMap((MapBlockBuilder) output, mapType, (Map<?, ?>) value, zoneId);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException cce) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("ClassCastException for type %s: %s with error %s", javaType.getSimpleName(), type, cce));
        }
    }

    public static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }

    public static long toTrinoTimestamp(String datetime)
    {
        Instant instant = toLocalDateTime(datetime).toInstant(UTC);
        return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            if (value instanceof Utf8) {
                type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
            }
            else if (value instanceof String) {
                type.writeSlice(output, utf8Slice((String) value));
            }
            else {
                type.writeSlice(output, utf8Slice(value.toString()));
            }
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                type.writeSlice(output, Slices.wrappedHeapBuffer((ByteBuffer) value));
            }
            else if (value instanceof SqlVarbinary sqlVarbinary) {
                type.writeSlice(output, Slices.wrappedBuffer(sqlVarbinary.getBytes()));
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled value type for REAL: %s type for %s: %s", value.getClass().getName(), type.getJavaType(), type));
            }
        }
        else if (type instanceof CharType charType) {
            String stringValue;
            if (value instanceof Utf8) {
                stringValue = ((Utf8) value).toString();
            }
            else if (value instanceof String) {
                stringValue = (String) value;
            }
            else {
                // Fallback: convert any other object to its string representation
                stringValue = value.toString();
            }
            // IMPORTANT: Char types are padded with trailing "space" characters to make up for length if the contents are lesser than defined length.
            verify(stringValue.length() == charType.getLength(), "Char type should have a size of " + charType.getLength());
            // Need to trim out trailing spaces as Slice representing Char should not have trailing spaces
            type.writeSlice(output, utf8Slice(stringValue.trim()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for Slice type %s with value %s", type.getTypeSignature(), value.getClass().getName()));
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            BigDecimal valueAsBigDecimal;
            if (value instanceof SqlDecimal sqlDecimal) {
                valueAsBigDecimal = sqlDecimal.toBigDecimal();
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Cannot process value of type %s for DecimalType %s. Expected SqlDecimal",
                                value.getClass().getName(),
                                type.getTypeSignature()));
            }

            Object trinoNativeDecimalValue = Decimals.encodeScaledValue(valueAsBigDecimal, decimalType.getScale());
            type.writeObject(output, trinoNativeDecimalValue);
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private static void writeArray(ArrayBlockBuilder output, List<?> value, ArrayType arrayType, ZoneId zoneId)
    {
        Type elementType = arrayType.getElementType();
        output.buildEntry(elementBuilder -> {
            for (Object element : value) {
                appendTo(elementType, element, elementBuilder, zoneId);
            }
        });
    }

    private static void writeRow(RowBlockBuilder output, RowType rowType, GenericRecord record, ZoneId zoneId)
    {
        List<RowType.Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                RowType.Field field = fields.get(index);
                appendTo(field.getType(), record.get(field.getName().orElse("field" + index)), fieldBuilders.get(index), zoneId);
            }
        });
    }

    private static void writeRow(RowBlockBuilder output, RowType rowType, List<?> list, ZoneId zoneId)
    {
        List<RowType.Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                RowType.Field field = fields.get(index);
                appendTo(field.getType(), list.get(index), fieldBuilders.get(index), zoneId);
            }
        });
    }

    private static void writeMap(MapBlockBuilder output, MapType mapType, Map<?, ?> value, ZoneId zoneId)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        output.buildEntry((keyBuilder, valueBuilder) -> {
            for (Map.Entry<?, ?> entry : value.entrySet()) {
                appendTo(keyType, entry.getKey(), keyBuilder, zoneId);
                appendTo(valueType, entry.getValue(), valueBuilder, zoneId);
            }
        });
    }

    static class AvroDecimalConverter
    {
        private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        BigDecimal convert(int precision, int scale, Object value)
        {
            Schema schema = new Schema.Parser().parse(format("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}", precision, scale));
            return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer) value, schema, schema.getLogicalType());
        }
    }
}
