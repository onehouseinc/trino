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

import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Type;

import java.util.Collections;
import java.util.List;

public class CastExpression
        implements HudiTrinoExpression
{
    private final Expression sourceExpression;
    private final Type targetType;

    public CastExpression(Expression sourceExpression, Type hudiTargetType)
    {
        this.sourceExpression = sourceExpression;
        this.targetType = hudiTargetType;
    }

    @Override
    public List<Expression> getChildren()
    {
        return Collections.singletonList(sourceExpression);
    }

    @Override
    public Type getDataType()
    {
        return targetType;
    }

    @Override
    public <T> T accept(HudiTrinoExpressionVisitor<T> exprVisitor)
    {
        return exprVisitor.visitCast(this);
    }

    @Override
    public String toString()
    {
        return "CAST(" + sourceExpression + " AS " + targetType + ")";
    }
}
