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

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hudi.internal.schema.Types;

public class HudiTypeConverter
{
    private HudiTypeConverter()
    {
    }

    public static org.apache.hudi.internal.schema.Type getType(Type trinoType)
    {
        return switch (trinoType) {
            case IntegerType _ -> Types.IntType.get();
            case BigintType _ -> Types.LongType.get();
            case BooleanType _ -> Types.BooleanType.get();
            case RealType _ -> Types.FloatType.get();
            case DoubleType _ -> Types.DoubleType.get();
            case VarcharType _ -> Types.StringType.get();
            case TimestampType _, TimestampWithTimeZoneType _ -> Types.TimestampType.get();
            case DateType _ -> Types.DateType.get();
            default -> throw new IllegalStateException("Unsupported type: " + trinoType);
        };
    }
}
