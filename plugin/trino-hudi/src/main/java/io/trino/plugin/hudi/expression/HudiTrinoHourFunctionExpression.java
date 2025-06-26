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
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.internal.schema.Type;

import java.util.List;

public class HudiTrinoHourFunctionExpression
        extends HudiTrinoFunctionExpression
{
    static final String TRINO_FN_NAME = "hour";

    private static final String HUDI_FN_NAME = "hour";

    public HudiTrinoHourFunctionExpression(List<Expression> arguments, Type returnType)
    {
        super(arguments, returnType);
    }

    @Override
    public String getName()
    {
        return HUDI_FN_NAME;
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
            return sourceFields.contains(sourceColumnName);
            // Function does not accept arguments, do not need to check for it
        }
        return false;
    }

    public static String getTrinoFunctionName()
    {
        return TRINO_FN_NAME;
    }
}
