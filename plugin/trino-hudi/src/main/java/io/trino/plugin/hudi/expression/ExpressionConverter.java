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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class ExpressionConverter
{
    private static final Logger log = Logger.get(ExpressionConverter.class);
    private static final String FN_YEAR = "year";
    private static final String FN_MONTH = "month";
    private static final String FN_DAY = "day";
    private static final String FN_HOUR = "hour";
    private static final String FN_DATE_FORMAT = "date_format";
    private static final String FN_FROM_UNIX = "from_unixtime";
    private static final String FN_SUBSTRING = "substring";

    private static final List<String> CANDIDATE_FUNCTIONS_FOR_EI = ImmutableList.of(FN_YEAR, FN_MONTH, FN_DAY, FN_HOUR, FN_DATE_FORMAT, FN_FROM_UNIX, FN_SUBSTRING);

    private final Set<Expression> candidateCollector;

    public ExpressionConverter(ConnectorExpression connectorExpression)
    {
        requireNonNull(connectorExpression, "connectorExpression is null");
        this.candidateCollector = new HashSet<>();
        convertExpressionRecursive(connectorExpression);
    }

    /**
     * Returns the list of candidate Hudi expressions collected during the conversion.
     *
     * @return An unmodifiable list of candidate expressions
     */
    public List<Expression> getCandidateExpressions()
    {
        return this.candidateCollector.stream().toList();
    }

    private Expression convertExpressionRecursive(ConnectorExpression connectorExpression)
    {
        requireNonNull(connectorExpression, "connectorExpression is null");

        Expression expression = switch (connectorExpression) {
            case Variable v -> convertTrinoVariable(v);
            case Constant c -> convertTrinoConstant(c);
            case Call call -> convertTrinoCall(call);
            default -> throw new IllegalStateException("Unexpected value: " + connectorExpression);
        };

        return expression;
    }

    private Expression convertTrinoVariable(Variable trinoVar)
    {
        String columnName = trinoVar.getName();
        return new NameReference(columnName);
    }

    private Expression convertTrinoConstant(Constant trinoConst)
    {
        Object value = trinoConst.getValue();
        Type constantType = HudiTypeConverter.getType(trinoConst.getType());
        Object literal;
        if (value instanceof Slice slice) {
            // Convert slice to string
            literal = slice.toStringUtf8();
        }
        else {
            literal = value;
        }
        return new Literal<>(literal, constantType);
    }

    private Expression convertTrinoCall(Call trinoCall)
    {
        FunctionName functionName = trinoCall.getFunctionName();
        List<Expression> convertedArguments = new ArrayList<>();
        for (ConnectorExpression arg : trinoCall.getArguments()) {
            convertedArguments.add(convertExpressionRecursive(arg));
        }

        return mapTrinoFunctionToCustomExpression(functionName, convertedArguments, trinoCall.getType());
    }

    private Expression mapTrinoFunctionToCustomExpression(
            FunctionName functionName, List<Expression> arguments, io.trino.spi.type.Type trinoType)
    {
        Type hudiReturnType = HudiTypeConverter.getType(trinoType);

        Expression finalHudiExpression;

        // Logical Operators
        if (StandardFunctions.AND_FUNCTION_NAME.equals(functionName)) {
            if (arguments.isEmpty()) {
                // Handle case of empty arguments for AND if necessary
                throw new IllegalArgumentException("AND function expects at least one argument.");
            }
            if (arguments.size() == 1) {
                // AND(X) is just X
                finalHudiExpression = arguments.getFirst();
            }
            // Iteratively build the AND chain
            Expression result = arguments.get(0);
            for (int i = 1; i < arguments.size(); i++) {
                result = Predicates.and(result, arguments.get(i));
            }
            finalHudiExpression = result;
        }
        else if (StandardFunctions.OR_FUNCTION_NAME.equals(functionName)) {
            if (arguments.isEmpty()) {
                // Handle case of empty arguments for OR if necessary
                throw new IllegalArgumentException("OR function expects at least one argument.");
            }
            if (arguments.size() == 1) {
                // OR(X) is just X
                return arguments.getFirst();
            }
            // Iteratively build the OR chain
            Expression result = arguments.getFirst();
            for (int i = 1; i < arguments.size(); i++) {
                result = Predicates.or(result, arguments.get(i));
            }
            finalHudiExpression = result;
        }
        else if (StandardFunctions.NOT_FUNCTION_NAME.equals(functionName) && arguments.size() == 1) {
            finalHudiExpression = Predicates.not(arguments.getFirst());
        }
        // Comparison Operators
        else if (StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName) && arguments.size() == 2) {
            finalHudiExpression = Predicates.eq(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(functionName) && arguments.size() == 2) {
            finalHudiExpression = Predicates.gt(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.equals(functionName) && arguments.size() == 2) {
            finalHudiExpression = Predicates.lt(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName) && arguments.size() == 2) {
            finalHudiExpression = Predicates.gteq(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName) && arguments.size() == 2) {
            finalHudiExpression = Predicates.lteq(arguments.get(0), arguments.get(1));
        }
        // Null checks
        else if (StandardFunctions.IS_NULL_FUNCTION_NAME.equals(functionName) && arguments.size() == 1) {
            finalHudiExpression = Predicates.isNull(arguments.getFirst());
        }
        // IN operator from Trino's perspective might be a call like "IN"(column, val1, val2...)
        // or "IN"(column, ARRAY[val1, val2...]) or it might be handled by TupleDomain primarily.
        // If it comes as a function:
        else if (StandardFunctions.IN_PREDICATE_FUNCTION_NAME.equals(functionName) && arguments.size() > 1) {
            // First argument is the column/expression, rest are values or an array
            finalHudiExpression = Predicates.in(arguments.getFirst(), arguments.subList(1, arguments.size()));
        }
        // Cast
        else if (StandardFunctions.CAST_FUNCTION_NAME.equals(functionName) && arguments.size() == 1) {
            Expression sourceExpression = arguments.getFirst();
            finalHudiExpression = new CastExpression(sourceExpression, hudiReturnType);
        }
        // Functions
        else if (FN_DAY.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = checkAndCreateExpression(FN_DAY, 1, arguments, hudiReturnType);
        }
        else if (FN_FROM_UNIX.equalsIgnoreCase(functionName.getName())) {
            // The number of arguments will determine which overload it is.
            // Trino's type system will provide the correct trinoReturnType based on the overload.
            // (e.g., TIMESTAMP for 1-arg, TIMESTAMP WITH TIME ZONE for 2/3-arg versions)
            // No need to force a correct returnType here; expression should handle it.
            finalHudiExpression = new HudiFunctionExpression(FN_FROM_UNIX, arguments, hudiReturnType);
        }
        else if (FN_YEAR.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = checkAndCreateExpression(FN_YEAR, 1, arguments, hudiReturnType);
        }
        else if (FN_MONTH.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = checkAndCreateExpression(FN_MONTH, 1, arguments, hudiReturnType);
        }
        else if (FN_HOUR.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = checkAndCreateExpression(FN_HOUR, 1, arguments, hudiReturnType);
        }
        else if (FN_DATE_FORMAT.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = new HudiFunctionExpression(FN_DATE_FORMAT, arguments, hudiReturnType);
        }
        else if (FN_SUBSTRING.equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = new HudiFunctionExpression(FN_SUBSTRING, arguments, hudiReturnType);
        }
        else {
            // TODO: represent as a generic function call if not mapped?
            log.warn("Unhandled Trino function: " + functionName);
            finalHudiExpression = new HudiFunctionExpression(functionName.getName(), arguments, hudiReturnType);
        }
        tryAddAsCandidate(finalHudiExpression);
        return finalHudiExpression;
    }

    private void tryAddAsCandidate(Expression expression)
    {
        if (isCandidateForExpressionIndex(expression)) {
            this.candidateCollector.add(expression);
        }
    }

    private static HudiFunctionExpression checkAndCreateExpression(String functionName, int argumentSize, List<Expression> arguments, Type returnType)
    {
        if (arguments.size() == argumentSize) {
            return new HudiFunctionExpression(functionName, arguments, returnType);
        }
        throw new TrinoException(INVALID_ARGUMENTS, getErrorMessage(FN_DAY, arguments.size()));
    }

    private static String getErrorMessage(String functionName, int argumentSize)
    {
        return "Trino " + functionName + " function expects 1 argument, but got " + argumentSize;
    }

    private static boolean isCandidateForExpressionIndex(Expression expression)
    {
        if (expression instanceof Predicates.BinaryComparison binaryComparison) {
            Expression left = binaryComparison.getLeft();
            if (left instanceof HudiFunctionExpression fnExpression) {
                return CANDIDATE_FUNCTIONS_FOR_EI.contains(fnExpression.getName());
            }
        }
        return false;
    }
}
