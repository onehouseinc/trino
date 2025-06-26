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
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.internal.schema.Type;

import java.util.List;
import java.util.Map;

public class HudiTrinoSubstringFunctionExpression
        extends HudiTrinoFunctionExpression
{
    static final String TRINO_FN_NAME = "substring";

    private static final String HUDI_FN_NAME = "substring";

    // This is mapped to the start argument in trino
    private final Object position;
    private final Object length;

    /**
     * The substring function has 2 forms:
     * 1. substring(string, start)
     * 2. substring(string, start, length)
     */
    public HudiTrinoSubstringFunctionExpression(List<Expression> arguments, Type returnType)
    {
        super(arguments, returnType);
        if (arguments.size() == 2) {
            this.position = getLiteralVal(arguments.get(1));
            this.length = null;
        }
        else if (arguments.size() == 3) {
            this.position = getLiteralVal(arguments.get(1));
            this.length = getLiteralVal(arguments.getLast());
        }
        else {
            // TODO: throw error here
            this.position = null;
            this.length = null;
        }
    }

    @Override
    public String getName()
    {
        return HUDI_FN_NAME;
    }

    private static Object getLiteralVal(Expression expression)
    {
        if (expression instanceof Literal lit) {
            return lit.getValue();
        }
        // TODO: throw error here
        return null;
    }

    @Override
    public boolean canUseIndex(HoodieIndexDefinition indexDefinition)
    {
        if (!super.canUseIndex(indexDefinition)) {
            return false;
        }

        List<String> sourceFields = indexDefinition.getSourceFields();
        if (getChildren().getFirst() instanceof NameReference argNameRef) {
            String sourceColumnName = argNameRef.getName();
            if (!sourceFields.contains(sourceColumnName)) {
                return false;
            }
            // Ensure that all arguments matches
            Map<String, String> indexOptions = indexDefinition.getIndexOptions();

            // a key is used to save the function name
            if (indexOptions.values().size() == 3) {
                return indexOptions.get("pos").equals(String.valueOf(position)) && indexOptions.get("len").equals(String.valueOf(length));
            }
            else if (indexOptions.values().size() == 2) {
                // Do not need to compare length
                return indexOptions.get("pos").equals(String.valueOf(position));
            }
            else {
                return false;
            }
        }
        return false;
    }

    public static String getTrinoFunctionName()
    {
        return TRINO_FN_NAME;
    }
}
