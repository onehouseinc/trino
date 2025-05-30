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
import org.apache.hudi.expression.ExpressionVisitor;

public interface HudiTrinoExpression
        extends Expression
{
    // Hacky way to get around ExpressionVisitor in hudi-common not having a visitFunction
    // This is done by extending ExpressionVisitor<T> to add the missing interface, then only implementing the method that accepts the extended interface
    <T> T accept(HudiTrinoExpressionVisitor<T> exprVisitor);

    @Override
    default <T> T accept(ExpressionVisitor<T> exprVisitor)
    {
        throw new UnsupportedOperationException("Cannot evaluate expression " + this);
    }
}
