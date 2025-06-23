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

import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Unit tests for the {@link ExpressionConverter} class.
 * This class verifies the correct conversion of Trino ConnectorExpressions to Hudi Predicates.
 */
public class ExpressionConverterTest
{
    private ExpressionConverter expressionConverter;

    // Mock objects for building expressions
    private final Variable mockTimestampVar = new Variable("ts", BIGINT);
    private final Variable mockColumnVar = new Variable("col", VarcharType.createVarcharType(10));
    private final NameReference hudiTimestampRef = new NameReference("ts");
    private final NameReference hudiColumnRef = new NameReference("col");

    /**
     * Provides arguments for testing all binary comparison operators.
     *
     * @return A stream of arguments: [Trino FunctionName, Hudi Predicate factory].
     */
    private static Stream<Arguments> binaryComparisonProvider()
    {
        return Stream.of(
                Arguments.of(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) Predicates::eq),
                Arguments.of(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) (l, r) -> Predicates.not(Predicates.eq(l, r))),
                Arguments.of(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) Predicates::gt),
                Arguments.of(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) Predicates::gteq),
                Arguments.of(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) Predicates::lt),
                Arguments.of(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (BiFunction<Expression, Expression, Predicate>) Predicates::lteq));
    }

    /**
     * Provides arguments for testing functions that are candidates for Expression Index.
     *
     * @return A stream of arguments: [Trino FunctionName, Hudi Expression Class].
     */
    private static Stream<Arguments> expressionIndexCandidateFunctionProvider()
    {
        return Stream.of(
                Arguments.of(HudiTrinoYearFunctionExpression.getTrinoFunctionName(), HudiTrinoYearFunctionExpression.class),
                Arguments.of(HudiTrinoMonthFunctionExpression.getTrinoFunctionName(), HudiTrinoMonthFunctionExpression.class),
                Arguments.of(HudiTrinoDayFunctionExpression.getTrinoFunctionName(), HudiTrinoDayFunctionExpression.class),
                Arguments.of(HudiTrinoHourFunctionExpression.getTrinoFunctionName(), HudiTrinoHourFunctionExpression.class),
                Arguments.of(HudiTrinoDateFormatFunctionExpression.getTrinoFunctionName(), HudiTrinoDateFormatFunctionExpression.class),
                Arguments.of(HudiTrinoFromUnixFunctionExpression.getTrinoFunctionName(), HudiTrinoFromUnixFunctionExpression.class),
                Arguments.of(HudiTrinoSubstringFunctionExpression.getTrinoFunctionName(), HudiTrinoSubstringFunctionExpression.class));
    }

    @BeforeEach
    void setUp()
    {
        // Initialize a new instance for each test
        expressionConverter = ExpressionConverter.getInstance();
    }

    @Test
    void testSingletonInstance()
    {
        // Ensures that getInstance() always returns the same object
        assertThat(ExpressionConverter.getInstance()).isSameAs(expressionConverter);
    }

    @Test
    void testConvertNullExpression()
    {
        // The main entry point should throw if the expression is null
        assertThatThrownBy(() -> expressionConverter.convert(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("connectorExpression is null");
    }

    @Test
    void testConvertVariable()
    {
        // Test conversion of a simple variable.
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Expression result = expressionConverter.convertExpressionRecursive(mockColumnVar, context);

        assertThat(result).isInstanceOf(NameReference.class);
        NameReference nameReference = (NameReference) result;
        assertThat(nameReference.getName()).isEqualTo("col");
        // No candidates should be generated for a simple variable
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testConvertConstant()
    {
        // Test conversion of a simple constant.
        Constant constant = new Constant(123L, BIGINT);
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Expression result = expressionConverter.convertExpressionRecursive(constant, context);

        assertThat(result).isInstanceOf(Literal.class);
        Literal<?> literal = (Literal<?>) result;
        assertThat(literal.getValue()).isEqualTo(123L);
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("binaryComparisonProvider")
    void testBinaryComparisonOperators(FunctionName functionName, BiFunction<Expression, Expression, Predicate> expectedPredicateBuilder)
    {
        // Tests operators like =, >, <, etc
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Constant literal = new Constant(100L, BIGINT);
        Call trinoCall = new Call(BOOLEAN, functionName, List.of(mockColumnVar, literal));

        Expression result = expressionConverter.convertExpressionRecursive(trinoCall, context);

        Predicate expected = expectedPredicateBuilder.apply(hudiColumnRef, new Literal<>(100L, HudiTypeConverter.getType(BIGINT)));
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(result.toString()).isEqualTo(expected.toString());
        // Simple comparisons are not candidates for Expression Index unless the left side is a function
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testLogicalAndOperator()
    {
        // Tests the AND operator with multiple arguments
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call greaterThan = new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant(10L, BIGINT)));
        Call lessThan = new Call(BOOLEAN, StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant(20L, BIGINT)));
        Call trinoCall = new Call(BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, List.of(greaterThan, lessThan));

        Expression result = expressionConverter.convertExpressionRecursive(trinoCall, context);

        Predicate expected = Predicates.and(
                Predicates.gt(hudiColumnRef, new Literal<>(10L, HudiTypeConverter.getType(BIGINT))),
                Predicates.lt(hudiColumnRef, new Literal<>(20L, HudiTypeConverter.getType(BIGINT))));
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(result.toString()).isEqualTo(expected.toString());
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testLogicalOrOperator()
    {
        // Tests the OR operator with multiple arguments
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call equal1 = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant("A", VarcharType.VARCHAR)));
        Call equal2 = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant("B", VarcharType.VARCHAR)));
        Call trinoCall = new Call(BOOLEAN, StandardFunctions.OR_FUNCTION_NAME, List.of(equal1, equal2));

        Expression result = expressionConverter.convertExpressionRecursive(trinoCall, context);

        Predicate expected = Predicates.or(
                Predicates.eq(hudiColumnRef, new Literal<>("A", HudiTypeConverter.getType(VarcharType.VARCHAR))),
                Predicates.eq(hudiColumnRef, new Literal<>("B", HudiTypeConverter.getType(VarcharType.VARCHAR))));
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(result.toString()).isEqualTo(expected.toString());
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testLogicalNotOperator()
    {
        // Tests the NOT operator
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call isNullCall = new Call(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, List.of(mockColumnVar));
        Call trinoCall = new Call(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, List.of(isNullCall));

        Expression result = expressionConverter.convertExpressionRecursive(trinoCall, context);

        Predicate expected = Predicates.not(Predicates.isNull(hudiColumnRef));
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(result.toString()).isEqualTo(expected.toString());
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testIsNullPredicate()
    {
        // Tests the IS NULL predicate
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call trinoCall = new Call(BOOLEAN, StandardFunctions.IS_NULL_FUNCTION_NAME, List.of(mockColumnVar));
        Expression result = expressionConverter.convertExpressionRecursive(trinoCall, context);
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(result.toString()).isEqualTo(Predicates.isNull(hudiColumnRef).toString());
        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("expressionIndexCandidateFunctionProvider")
    void testCandidateFunctionExtraction(String functionName, Class<? extends Expression> expectedClass)
    {
        // Tests that predicates involving specific functions are correctly identified as candidates.
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call comparisonCall = getCall(functionName);

        // Convert the entire expression
        Expression result = expressionConverter.convertExpressionRecursive(comparisonCall, context);

        // Verify the structure of the converted expression
        assertThat(result).isInstanceOf(Predicates.BinaryComparison.class);
        Predicates.BinaryComparison binaryComparison = (Predicates.BinaryComparison) result;
        assertThat(binaryComparison.getLeft()).isInstanceOf(expectedClass);
        assertThat(binaryComparison.getRight()).isInstanceOf(Literal.class);

        // Crucially, verify that this predicate was added to the candidate list.
        assertThat(context.getCandidateExpressions()).hasSize(1);
        assertThat(context.getCandidateExpressions().getFirst()).isEqualTo(result);
    }

    /**
     * Utility method for the test case: #testCandidateFunctionExtraction
     */
    private Call getCall(String functionName)
    {
        Type returnType = functionName.equals("date_format") || functionName.equals("substring") ? VarcharType.VARCHAR : INTEGER;

        // Mock the arguments for the functions
        List<ConnectorExpression> args;
        if (functionName.equals("date_format")) {
            args = List.of(mockTimestampVar, new Constant("%Y-%m-%d", VarcharType.VARCHAR));
        }
        else if (functionName.equals("substring")) {
            args = List.of(mockColumnVar, new Constant(1L, BIGINT), new Constant(3L, BIGINT));
        }
        else {
            args = List.of(mockTimestampVar);
        }

        Call functionCall = new Call(returnType, new FunctionName(functionName), args);

        // Create a comparison predicate, e.g., year(ts) = 2023
        Constant comparisonValue = new Constant(2023L, BIGINT);
        return new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(functionCall, comparisonValue));
    }

    @Test
    void testNonCandidateFunctionIsNotExtracted()
    {
        // Ensures that a standard comparison is not added to the candidate list
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call trinoCall = new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant(100L, BIGINT)));

        expressionConverter.convertExpressionRecursive(trinoCall, context);

        assertThat(context.getCandidateExpressions()).isEmpty();
    }

    @Test
    void testCandidateInsideNotIsNotExtracted()
    {
        // A candidate predicate wrapped in NOT should still be identified
        // Example: NOT (year(ts) = 2023)
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();
        Call yearCall = new Call(INTEGER, new FunctionName("year"), List.of(mockTimestampVar));
        Call equalsCall = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(yearCall, new Constant(2023L, BIGINT)));
        Call notCall = new Call(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, List.of(equalsCall));

        Expression result = expressionConverter.convertExpressionRecursive(notCall, context);

        // The result is a NOT predicate
        assertThat(result).isInstanceOf(Predicates.Not.class);
        // The candidate list should contain the inner predicate (the NOT itself) ONLY
        assertThat(context.getCandidateExpressions()).hasSize(1);
        assertThat(context.getCandidateExpressions().getFirst()).isEqualTo(result);
    }

    @Test
    void testComplexExpressionWithMultipleCandidates()
    {
        // This is the main integration test for the public `convert` method
        // Expression: year(ts) = 2023 AND col > 'abc' OR NOT(month(ts) = 5)

        // year(ts) = 2023
        Call finalOrExpression = getFinalOrExpression();

        // Perform the conversion
        List<Predicate> candidates = expressionConverter.convert(finalOrExpression);

        // Build the expected candidates
        HudiTrinoYearFunctionExpression hudiYearFunc = new HudiTrinoYearFunctionExpression(List.of(hudiTimestampRef), HudiTypeConverter.getType(INTEGER));
        Predicate expectedCandidate1 = Predicates.eq(hudiYearFunc, new Literal<>(2023L, HudiTypeConverter.getType(BIGINT)));

        HudiTrinoMonthFunctionExpression hudiMonthFunc = new HudiTrinoMonthFunctionExpression(List.of(hudiTimestampRef), HudiTypeConverter.getType(INTEGER));
        Predicate innerMonthPredicate = Predicates.eq(hudiMonthFunc, new Literal<>(5L, HudiTypeConverter.getType(BIGINT)));
        Predicate expectedCandidate2 = Predicates.not(innerMonthPredicate);

        // Assert that ONLY the two candidates were extracted
        assertThat(candidates).hasSize(2);
        // TODO: Predicates#Equals not implemented, do string matching for now
        assertThat(candidates.toString()).contains(expectedCandidate1.toString(), expectedCandidate2.toString());
    }

    /**
     * Utility method for the test case: #testComplexExpressionWithMultipleCandidates
     */
    private Call getFinalOrExpression()
    {
        Call yearCall = new Call(INTEGER, new FunctionName("year"), List.of(mockTimestampVar));
        Call yearEquals = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(yearCall, new Constant(2023L, BIGINT)));

        // col > 'abc'
        Call greaterThan = new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar, new Constant("abc", VarcharType.VARCHAR)));

        // AND expression part
        Call andExpression = new Call(BOOLEAN, StandardFunctions.AND_FUNCTION_NAME, List.of(yearEquals, greaterThan));

        // NOT(month(ts) = 5)
        Call monthCall = new Call(INTEGER, new FunctionName("month"), List.of(mockTimestampVar));
        Call monthEquals = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(monthCall, new Constant(5L, BIGINT)));
        Call notExpression = new Call(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, List.of(monthEquals));

        // Final OR expression
        Call finalOrExpression = new Call(BOOLEAN, StandardFunctions.OR_FUNCTION_NAME, List.of(andExpression, notExpression));
        return finalOrExpression;
    }

    @Test
    void testArgumentChecksForFunctions()
    {
        // Ensures that preconditions for argument counts are enforced
        ExpressionConverter.ConversionContext context = new ExpressionConverter.ConversionContext();

        // Test NOT with two arguments
        Call invalidNot = new Call(BOOLEAN, StandardFunctions.NOT_FUNCTION_NAME, List.of(mockColumnVar, mockTimestampVar));
        assertThatThrownBy(() -> expressionConverter.convertExpressionRecursive(invalidNot, context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("NOT function expects ONLY ONE argument.");

        // Test EQUAL with one argument
        Call invalidEqual = new Call(BOOLEAN, StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List.of(mockColumnVar));
        assertThatThrownBy(() -> expressionConverter.convertExpressionRecursive(invalidEqual, context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("EQUAL function expects ONLY TWO arguments.");

        // Test YEAR with zero arguments
        Call invalidYear = new Call(INTEGER, new FunctionName("year"), List.of());
        assertThatThrownBy(() -> expressionConverter.convertExpressionRecursive(invalidYear, context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expects 1 argument, but got 0");
    }
}
