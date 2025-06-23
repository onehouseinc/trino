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

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Type;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HudiTrinoFunctionExpression
        implements HudiTrinoExpression
{
    private final List<Expression> arguments;
    private final Type returnType;

    public HudiTrinoFunctionExpression(List<Expression> arguments, Type returnType)
    {
        // Store an immutable copy or an unmodifiable view
        this.arguments = arguments != null ? List.copyOf(arguments) : Collections.emptyList();
        this.returnType = returnType;
    }

    public String getName()
    {
        return HudiTrinoFunctionExpression.class.getSimpleName();
    }

    @Override
    public List<Expression> getChildren()
    {
        return arguments;
    }

    @Override
    public Type getDataType()
    {
        return returnType;
    }

    @Override
    public <T> T accept(HudiTrinoExpressionVisitor<T> exprVisitor)
    {
        return exprVisitor.visitFunction(this);
    }

    @Override
    public String toString()
    {
        return getName() + arguments.stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
    }

    public boolean canUseIndex(HoodieIndexDefinition indexDefinition)
    {
        return getName().equals(indexDefinition.getIndexFunction());
    }

    /**
     * There might be cases where the Trino litVal returns a long type, but stored column stats uses int to represent the value.
     * This helper method helps to cast it Tsrino's equivalent type.
     */
    public Object castToTrinoType(Object stat)
    {
        // Base case, just pass through
        return stat;
    }
}
