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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ExpressionConverter
{
    private static final Logger log = Logger.get(ExpressionConverter.class);

    private static final List<String> CANDIDATE_FUNCTIONS_FOR_EI = ImmutableList.of(
            HudiTrinoYearFunctionExpression.getTrinoFunctionName(),
            HudiTrinoMonthFunctionExpression.getTrinoFunctionName(),
            HudiTrinoDayFunctionExpression.getTrinoFunctionName(),
            HudiTrinoHourFunctionExpression.getTrinoFunctionName(),
            HudiTrinoDateFormatFunctionExpression.getTrinoFunctionName(),
            HudiTrinoFromUnixFunctionExpression.getTrinoFunctionName(),
            HudiTrinoSubstringFunctionExpression.getTrinoFunctionName());

    // The single, static instance of the ExpressionConverter.
    private static final ExpressionConverter INSTANCE = new ExpressionConverter();

    /**
     * Private constructor to prevent instantiation from outside the class
     */
    private ExpressionConverter()
    {
    }

    /**
     * Returns the singleton instance of the ExpressionConverter
     *
     * @return The singleton instance
     */
    public static ExpressionConverter getInstance()
    {
        return INSTANCE;
    }

    /**
     * Converts a Trino ConnectorExpression into a list of candidate Hudi Predicates.
     * This is the main entry point for the conversion process.
     *
     * @param connectorExpression The Trino expression to convert.
     * @return An unmodifiable list of candidate Hudi Predicate expressions.
     */
    public List<Predicate> convert(ConnectorExpression connectorExpression)
    {
        requireNonNull(connectorExpression, "connectorExpression is null");
        ConversionContext context = new ConversionContext();
        convertExpressionRecursive(connectorExpression, context);
        return context.getCandidateExpressions();
    }

    /**
     * A helper class to hold the state of a single conversion operation.
     * This ensures that concurrent conversions do not interfere with each other.
     */
    @VisibleForTesting
    static class ConversionContext
    {
        private final Set<Predicate> candidateCollector = new HashSet<>();

        /**
         * Recursively checks if potentialDescendant is a descendant of the parent expression.
         * This is used to avoid adding redundant sub-expressions to the candidate list.
         *
         * @param parent The parent expression.
         * @param potentialDescendant The expression to check for ancestry.
         * @return true if potentialDescendant is a descendant of parent, false otherwise.
         */
        private boolean isDescendant(Expression parent, Expression potentialDescendant)
        {
            if (parent.getChildren() == null) {
                return false;
            }
            for (Expression child : parent.getChildren()) {
                // Check if the immediate child is the one we're looking for, or recursively check its children
                if (child.equals(potentialDescendant) || isDescendant(child, potentialDescendant)) {
                    return true;
                }
            }
            return false;
        }

        void tryAddCandidate(Expression expression)
        {
            if (expression instanceof Predicate newCandidate && isCandidateForExpressionIndex(newCandidate)) {
                // If a new candidate is found, remove any existing candidates that are its children
                // This ensures we only keep the largest, most complete predicate
                // For example, if we have `year(ts) = 2023` and now process `NOT (year(ts) = 2023)`, we should prefer the latter and remove the former
                candidateCollector.removeIf(existingCandidate -> isDescendant(newCandidate, existingCandidate));

                // As a safeguard, do not add this new candidate if it's already a descendant of another, larger candidate that has already been processed
                boolean isAlreadyCovered = candidateCollector.stream()
                        .anyMatch(existingCandidate -> isDescendant(existingCandidate, newCandidate));

                if (!isAlreadyCovered) {
                    this.candidateCollector.add(newCandidate);
                }
            }
        }

        List<Predicate> getCandidateExpressions()
        {
            return new ArrayList<>(this.candidateCollector);
        }
    }

    /**
     * Recursively converts a Trino ConnectorExpression into a Hudi Expression.
     *
     * @param connectorExpression The Trino expression to convert
     * @param context The context for the current conversion operation
     * @return The corresponding Hudi Expression
     */
    @VisibleForTesting
    Expression convertExpressionRecursive(ConnectorExpression connectorExpression, ConversionContext context)
    {
        requireNonNull(connectorExpression, "connectorExpression is null");

        return switch (connectorExpression) {
            case Variable v -> convertTrinoVariable(v);
            case Constant c -> convertTrinoConstant(c);
            case Call call -> convertTrinoCall(call, context);
            default -> throw new IllegalStateException("Unexpected value: " + connectorExpression);
        };
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

    private Expression convertTrinoCall(Call trinoCall, ConversionContext context)
    {
        FunctionName functionName = trinoCall.getFunctionName();
        List<Expression> convertedArguments = new ArrayList<>();
        for (ConnectorExpression arg : trinoCall.getArguments()) {
            convertedArguments.add(convertExpressionRecursive(arg, context));
        }

        return mapTrinoFunctionToCustomExpression(functionName, convertedArguments, trinoCall.getType(), context);
    }

    private Expression mapTrinoFunctionToCustomExpression(FunctionName functionName, List<Expression> arguments, io.trino.spi.type.Type trinoType, ConversionContext context)
    {
        Type hudiReturnType = HudiTypeConverter.getType(trinoType);

        Expression finalHudiExpression;

        // Logical Operators
        if (StandardFunctions.AND_FUNCTION_NAME.equals(functionName)) {
            checkArgument(!arguments.isEmpty(), "AND function expects at least one argument.");
            if (arguments.size() == 1) {
                // AND(X) is just X
                return arguments.getFirst();
            }
            // Iteratively build the AND chain
            Expression result = arguments.getFirst();
            for (int i = 1; i < arguments.size(); i++) {
                result = Predicates.and(result, arguments.get(i));
            }
            finalHudiExpression = result;
        }
        else if (StandardFunctions.OR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(!arguments.isEmpty(), "OR function expects at least one argument.");
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
        else if (StandardFunctions.NOT_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 1, "NOT function expects ONLY ONE argument.");
            finalHudiExpression = Predicates.not(arguments.getFirst());
        }
        // Comparison Operators
        else if (StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "EQUAL function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.eq(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "NOT_EQUAL function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.not(Predicates.eq(arguments.get(0), arguments.get(1)));
        }
        else if (StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "GREATER_THAN function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.gt(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "LESS_THAN function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.lt(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "GREATER_THAN_OR_EQUAL function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.gteq(arguments.get(0), arguments.get(1));
        }
        else if (StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 2, "LESS_THAN_OR_EQUAL function expects ONLY TWO arguments.");
            finalHudiExpression = Predicates.lteq(arguments.get(0), arguments.get(1));
        }
        // Null checks
        else if (StandardFunctions.IS_NULL_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 1, "IS_NULL function expects ONLY ONE argument.");
            finalHudiExpression = Predicates.isNull(arguments.getFirst());
        }
        // IN operator from Trino's perspective might be a call like "IN"(column, val1, val2...)
        // or "IN"(column, ARRAY[val1, val2...]) or it might be handled by TupleDomain primarily.
        // If it comes as a function:
        else if (StandardFunctions.IN_PREDICATE_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() > 1, "IN_PREDICATE function expects AT LEAST ONE argument.");

            // First argument is the column/expression, rest are values or an array
            Expression columnOrValue = arguments.get(0);
            Expression valueListProvider = arguments.get(1);

            List<Expression> values;
            if (valueListProvider instanceof Literal && ((Literal<?>) valueListProvider).getValue() instanceof Collection) {
                // The value list is a Literal containing a collection (from an unfolded ARRAY constructor)
                values = (List<Expression>) ((Literal<?>) valueListProvider).getValue();
            }
            else {
                // The value list is a list of constants, not from an ARRAY constructor, this is included for robustness
                values = List.of(valueListProvider);
            }
            finalHudiExpression = Predicates.in(columnOrValue, values);
        }
        // Trino's ARRAY constructor call
        else if (StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME.equals(functionName)) {
            // The `arguments` list already contains Hudi Expressions (Literals) for each element of the array.
            // We just package this list into a single Literal<List<Expression>> to be consumed by the IN predicate handler.
            // This is a transient object and does not represent a valid Hudi Literal type.
            finalHudiExpression = new Literal<>(arguments, hudiReturnType);
        }
        // Cast
        else if (StandardFunctions.CAST_FUNCTION_NAME.equals(functionName)) {
            checkArgument(arguments.size() == 1, "CAST function expects ONLY ONE argument.");
            Expression sourceExpression = arguments.getFirst();
            finalHudiExpression = new CastExpression(sourceExpression, hudiReturnType);
        }
        // Functions
        else if (HudiTrinoDayFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            checkArgument(arguments.size() == 1, "Trino " + functionName + " function expects 1 argument, but got " + arguments.size());
            return new HudiTrinoDayFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoFromUnixFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            // The number of arguments will determine which overload it is.
            // Trino's type system will provide the correct trinoReturnType based on the overload.
            // (e.g., TIMESTAMP for 1-arg, TIMESTAMP WITH TIME ZONE for 2/3-arg versions)
            // No need to force a correct returnType here; expression should handle it.
            finalHudiExpression = new HudiTrinoFromUnixFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoYearFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            checkArgument(arguments.size() == 1, "Trino " + functionName + " function expects 1 argument, but got " + arguments.size());
            finalHudiExpression = new HudiTrinoYearFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoMonthFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            checkArgument(arguments.size() == 1, "Trino " + functionName + " function expects 1 argument, but got " + arguments.size());
            finalHudiExpression = new HudiTrinoMonthFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoHourFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            checkArgument(arguments.size() == 1, "Trino " + functionName + " function expects 1 argument, but got " + arguments.size());
            finalHudiExpression = new HudiTrinoHourFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoDateFormatFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = new HudiTrinoDateFormatFunctionExpression(arguments, hudiReturnType);
        }
        else if (HudiTrinoSubstringFunctionExpression.getTrinoFunctionName().equalsIgnoreCase(functionName.getName())) {
            finalHudiExpression = new HudiTrinoSubstringFunctionExpression(arguments, hudiReturnType);
        }
        else {
            // TODO: represent as a generic function call if not mapped?
            log.warn("Unhandled Trino function: " + functionName);
            finalHudiExpression = new HudiTrinoFunctionExpression(arguments, hudiReturnType);
        }
        // Try to add the resulting expression to our candidate list for pushdown.
        context.tryAddCandidate(finalHudiExpression);
        return finalHudiExpression;
    }

    /**
     * Checks if a given Predicate is a candidate for the Expression Index (EI).
     * This is determined by checking if the predicate is a binary comparison where the left side is one of the predefined candidate functions.
     *
     * @param predicate The predicate to check
     * @return True if the predicate is a candidate for EI, false otherwise
     */
    private static boolean isCandidateForExpressionIndex(Predicate predicate)
    {
        // Handle NOT predicates by recursively checking their child
        if (predicate instanceof Predicates.Not notPredicate) {
            checkArgument(notPredicate.getChildren().size() == 1, "Not predicate should contain ONLY 1 argument.");
            Expression child = notPredicate.getChildren().getFirst();
            if (child instanceof Predicate) {
                return isCandidateForExpressionIndex((Predicate) child);
            }
            return false;
        }

        if (predicate instanceof Predicates.BinaryComparison binaryComparison) {
            Expression left = binaryComparison.getLeft();
            if (left instanceof HudiTrinoFunctionExpression fnExpression) {
                return CANDIDATE_FUNCTIONS_FOR_EI.contains(fnExpression.getName());
            }
        }
        return false;
    }
}
