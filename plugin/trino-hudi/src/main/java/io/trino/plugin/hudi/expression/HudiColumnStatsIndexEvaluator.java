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

import io.trino.spi.TrinoException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.expression.BoundReference;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.NameReference;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Type;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class HudiColumnStatsIndexEvaluator
        implements HudiTrinoExpressionVisitor<Boolean>
{
    // Temporary holder for visitor
    private HoodieMetadataColumnStats stats;

    // Cache the comparators for type
    private final Map<Type.TypeID, Comparator<?>> comparators = new HashMap<>();

    public HudiColumnStatsIndexEvaluator() {}

    private Comparator getComparator(Literal literal)
    {
        // Comparator will be initialized based on column type in methods where needed and cached
        Comparator comparator = comparators.computeIfAbsent(literal.getDataType().typeId(), _ -> Comparator.naturalOrder());
        comparators.put(literal.getDataType().typeId(), comparator);
        return comparator;
    }

    public void setStats(HoodieMetadataColumnStats stats)
    {
        this.stats = stats;
    }

    @Override
    public Boolean visitFunction(HudiTrinoFunctionExpression function)
    {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support visiting functions");
    }

    @Override
    public Boolean visitCast(CastExpression castExpression)
    {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support visiting casts");
    }

    @Override
    public Boolean alwaysTrue()
    {
        return true;
    }

    @Override
    public Boolean alwaysFalse()
    {
        return false;
    }

    @Override
    public Boolean visitLiteral(Literal literal)
    {
        // By right, this should not be reachable and supported, since it is a literal, try evaluating booleans nonetheless
        if (literal.getValue() instanceof Boolean bool) {
            return bool;
        }
        // A non-boolean literal itself isn't a filter condition that stats can evaluate
        throw new UnsupportedOperationException("Cannot use column stats to evaluate a non-boolean literal predicate: " + literal);
    }

    @Override
    public Boolean visitNameReference(NameReference attribute)
    {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support visiting name references");
    }

    @Override
    public Boolean visitBoundReference(BoundReference boundReference)
    {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support visiting bound references");
    }

    @Override
    public Boolean visitAnd(Predicates.And and)
    {
        return and.getLeft().accept(this) && and.getRight().accept(this);
    }

    @Override
    public Boolean visitOr(Predicates.Or or)
    {
        return or.getLeft().accept(this) || or.getRight().accept(this);
    }

    @Override
    public Boolean visitPredicate(Predicate predicate)
    {
        if (predicate instanceof Predicates.BinaryComparison comp) {
            return visitBinaryComparison(comp);
        }
        else if (predicate instanceof Predicates.Not not) {
            return visitNot(not);
        }
        return true;
    }

    public Boolean visitBinaryComparison(Predicates.BinaryComparison comp)
    {
        Expression left = comp.getLeft();
        Expression right = comp.getRight();

        HudiTrinoFunctionExpression columnRef;
        Literal<?> literal;
        Expression.Operator directOperator = comp.getOperator();

        if (left instanceof HudiTrinoFunctionExpression && right instanceof Literal) {
            columnRef = (HudiTrinoFunctionExpression) left;
            literal = (Literal<?>) right;
        }
        else if (right instanceof HudiTrinoFunctionExpression && left instanceof Literal) {
            // For cases where NameReference is on the RHS and lit is on the LHS
            columnRef = (HudiTrinoFunctionExpression) right;
            literal = (Literal<?>) left;
            // Flip operator for lit `op` col to be col `newOp` lit
            switch (comp.getOperator()) {
                case GT:
                    directOperator = Expression.Operator.LT;
                    break;
                case LT:
                    directOperator = Expression.Operator.GT;
                    break;
                case GT_EQ:
                    directOperator = Expression.Operator.LT_EQ;
                    break;
                case LT_EQ:
                    directOperator = Expression.Operator.GT_EQ;
                    break;
                case EQ:
                    directOperator = Expression.Operator.EQ;
                    break;
                default:
                    return true; // Unknown operator or not a simple comparison, conservative
            }
        }
        else {
            // Comparison is not Column vs Literal, or involves complex expressions on one side
            // Stats cannot help directly, handle all other cases here by being conservative
            // Examples of cases that will reach this branch are: `col_int > 2`
            return true;
        }

        // If all values in the stat range are NULL, comparisons with non-null literals are false
        // (Unless the literal itself is null, which should be handled by IS NULL)
        if (literal.getValue() != null && stats.getValueCount() > 0 && Objects.equals(stats.getNullCount(), stats.getValueCount())) {
            return false;
        }

        Object statMin = columnRef.castToTrinoType(((GenericRecord) stats.getMinValue()).get(0));
        Object statMax = columnRef.castToTrinoType(((GenericRecord) stats.getMaxValue()).get(0));
        Object litVal = literal.getValue();

        if (litVal == null) {
            // If the operator is EQ for "col = NULL", it means "col IS NULL"
            if (directOperator == Expression.Operator.EQ) {
                return stats.getNullCount() > 0;
            }
            // For other comparisons with NULL literal
            // Comparisons like col > NULL are typically false
            return false;
        }

        // Perform HoodieColumnStatsIndex evaluation for non-null values
        Comparator<Object> typeComparator = getComparator(literal);

        switch (directOperator) {
            case EQ:
                // For EQ, the literal must be within [min, max] inclusive
                // Prune if litVal < statMin OR litVal > statMax
                // If statMin is null, litVal < statMin is false. If statMax is null, litVal > statMax is false.
                if (statMin != null && typeComparator.compare(litVal, statMin) < 0) {
                    return false;
                }
                if (statMax != null && typeComparator.compare(litVal, statMax) > 0) {
                    return false;
                }
                // litVal is within [statMin, statMax] or bounds are open/null
                return true;
            case GT:
                // For col > litVal, prune if statMax <= litVal
                // If statMax is null (unbounded above), cannot prune unless litVal is also null (handled)
                if (statMax != null && typeComparator.compare(statMax, litVal) <= 0) {
                    return false;
                }
                return true;
            case GT_EQ:
                // For col >= litVal, prune if statMax < litVal
                if (statMax != null && typeComparator.compare(statMax, litVal) < 0) {
                    return false;
                }
                return true;
            case LT:
                // For col < litVal, prune if statMin >= litVal
                // If statMin is null (unbounded below), cannot prune unless litVal is also null
                if (statMin != null && typeComparator.compare(statMin, litVal) >= 0) {
                    return false;
                }
                return true;
            case LT_EQ:
                // For col <= litVal, prune if statMin > litVal.
                if (statMin != null && typeComparator.compare(statMin, litVal) > 0) {
                    return false;
                }
                return true;
            default:
                return true;
        }
    }

    /**
     * Evaluates a {@link Predicates.Not} expression to determine if it can be pruned based on column statistics.
     *
     * <p><strong>Important Note on `NOT (BinaryComparison)`:</strong></p>
     * <p>This method cannot be simply implemented as {@code !visitBinaryComparison(originalComparison)}.
     * The distinction is crucial for correct pruning when a predicate's outcome is uncertain based on the available statistics.</p>
     *
     * <ul>
     * <li>{@code visitBinaryComparison(P)} returns {@code true} if predicate {@code P} <em>might be true</em>
     * (i.e., stats do not allow us to definitively disprove {@code P}). It returns {@code false} only if {@code P} is <em>definitely false</em>.</li>
     *
     * <li>If {@code P} is uncertain (e.g., a literal falls within a column's min/max range but doesn't span the entire range, so {@code P} could be true for some rows and
     * false for others), then:
     * <ul>
     * <li>{@code visitBinaryComparison(P)} would return {@code true} (cannot prune {@code P}).</li>
     * <li>{@code !visitBinaryComparison(P)} would then yield {@code false}. If this {@code false} were taken as the result for evaluating {@code NOT P}, it would incorrectly
     * imply that {@code NOT P} is <em>definitely false</em>, which means {@code P} must be <em>definitely true</em>. This is an invalid conclusion if {@code P} was merely
     * uncertain.</li>
     * </ul>
     * </li>
     *
     * <li>This method, {@code visitNot(NOT P)}, correctly handles this by determining if {@code NOT P} <em>might be true</em>. If {@code P} is uncertain, then {@code NOT P} is
     * also uncertain. In such cases, this method should return {@code true} (cannot prune based on {@code NOT P}).</li>
     * </ul>
     *
     * <p><strong>Example:</strong> Consider predicate {@code P = (age > 30)} with column stats {@code minAge=20, maxAge=40}.</p>
     * <ul>
     * <li>{@code visitBinaryComparison(age > 30)} returns {@code true} (as {@code age > 30} is uncertain but possible).</li>
     * <li>{@code !visitBinaryComparison(age > 30)} would yield {@code false}.</li>
     * <li>{@code visitNot(NOT (age > 30))}, which effectively evaluates if {@code age <= 30} might be true, correctly returns {@code true} (as {@code age <= 30} is also
     * uncertain but possible within the stats).</li>
     * </ul>
     *
     * <p>For binary comparisons, this method typically negates the operator (e.g., {@code >} becomes {@code <=}) and re-evaluates, or applies specific logic for operators like
     * {@code EQ}.</p>
     *
     * @param not The {@link Predicates.Not} expression to evaluate.
     * @return {@code true} if the Not predicate might be true (cannot prune), {@code false} if the Not predicate is definitely false (can prune)
     */
    private Boolean visitNot(Predicates.Not not)
    {
        // Will only have 1 child
        Expression child = not.getChildren().getFirst();

        if (child instanceof Predicates.BinaryComparison bc) {
            Expression.Operator op = bc.getOperator();
            Expression.Operator negatedOp = null;

            switch (op) {
                case GT:
                    negatedOp = Expression.Operator.LT_EQ;
                    break;
                case LT:
                    negatedOp = Expression.Operator.GT_EQ;
                    break;
                case GT_EQ:
                    negatedOp = Expression.Operator.LT;
                    break;
                case LT_EQ:
                    negatedOp = Expression.Operator.GT;
                    break;
                case EQ:
                    // Handle NOT (X EQ Y)
                    // This is true if (X EQ Y) is false
                    // This is false if (X EQ Y) is true
                    Expression left = bc.getLeft();
                    Expression right = bc.getRight();
                    HudiTrinoFunctionExpression columnRef;
                    Literal<?> literal;

                    if (left instanceof HudiTrinoFunctionExpression && right instanceof Literal) {
                        columnRef = (HudiTrinoFunctionExpression) left;
                        literal = (Literal<?>) right;
                    }
                    else if (right instanceof HudiTrinoFunctionExpression && left instanceof Literal) {
                        // Order does not matter for EQ
                        columnRef = (HudiTrinoFunctionExpression) right;
                        literal = (Literal<?>) left;
                    }
                    else {
                        // Handle all other cases where RHS/LHS does not contain a HudiTrinoFunctionExpression
                        // Conservatively return true
                        return true;
                    }

                    Object litVal = literal.getValue();
                    if (litVal == null) {
                        // NOT (col IS NULL) -> col IS NOT NULL
                        return new Predicates.IsNotNull(columnRef).accept(this);
                    }

                    // If all values are NULL in stats, then col = litVal (non-null) is false, so NOT (col=litVal) is true
                    if (stats.getValueCount() > 0 && Objects.equals(stats.getNullCount(), stats.getValueCount())) {
                        return true;
                    }

                    Object statMin = ((GenericRecord) stats.getMinValue()).get(0);
                    Object statMax = ((GenericRecord) stats.getMaxValue()).get(0);
                    Comparator<Object> typeComparator = getComparator(literal);

                    /*
                     * Below is a simplified way of writing, but not really readable:
                     * (All non-nulls are equal to litVal OR all values are litVal)
                     * statMin != null && statMax != null &&
                     *  typeComparator.compare(litVal, statMin) == 0 &&
                     *  typeComparator.compare(litVal, statMax) == 0 &&
                     *  // there are non-nulls
                     *  (stats.getValueCount() - stats.getNullCount() > 0) &&
                     *  (stats.getNullCount() == 0 || stats.getValueCount() - stats.getNullCount() == stats.getValueCount());
                     */
                    boolean definitelyEqual;

                    // If litVal is within [min,max] and min != max, then EQ could be true or false. So NOT EQ could be true.
                    // The only time NOT EQ is definitely false is if EQ is definitely true (e.g. min=max=litVal)
                    // If min=max=litval, then EQ is true, so NOT EQ is false
                    // A simpler definite check: min=max=litval and no nulls (if litval not null)
                    if (stats.getNullCount() == 0 && statMin != null && statMin.equals(statMax) && statMin.equals(litVal)) {
                        definitelyEqual = true;
                    }
                    else {
                        // More general check for "definitelyEqual" is hard without knowing distribution
                        // For now, if litVal is outside [min,max] range, then EQ is false, so NOT EQ is true
                        if ((statMin != null && typeComparator.compare(litVal, statMin) < 0) ||
                                (statMax != null && typeComparator.compare(litVal, statMax) > 0)) {
                            return true; // EQ is definitely false, so NOT EQ is true
                        }
                        // If litVal is within [min,max] and min!=max, then EQ could be true or false. So NOT EQ could be true.
                        // The only time NOT EQ is definitely false is if EQ is definitely true (e.g. min=max=litVal)
                        // Conservatively, if not definitely equal, then NOT EQ might be true
                        // Attempt to be conservative
                        // If min=max=litval, then EQ is true, so NOT EQ is false
                        if (statMin != null && statMin.equals(litVal) && statMax != null && statMax.equals(litVal) && stats.getNullCount() == 0) {
                            return false; // EQ is definitely true, so NOT EQ is false
                        }
                        return true; // Otherwise, NOT EQ could be true.
                    }
                    // Will always be false but let's keep it this way to make it more readable
                    return !definitelyEqual;
                default:
                    throw new TrinoException(NOT_SUPPORTED, op + " is not supported in binary comparison evaluation");
            }
            if (negatedOp != null) {
                // Create a new BinaryComparison with the negated operator and evaluate it.
                return new Predicates.BinaryComparison(bc.getLeft(), negatedOp, bc.getRight()).accept(this);
            }
        }
        else if (child instanceof Predicates.IsNull) {
            return new Predicates.IsNotNull(child.getChildren().getFirst()).accept(this);
        }
        else if (child instanceof Predicates.IsNotNull) {
            return new Predicates.IsNull(child.getChildren().getFirst()).accept(this);
        }
        else if (child instanceof Predicates.TrueExpression) {
            return false; // NOT TRUE is FALSE
        }
        else if (child instanceof Predicates.FalseExpression) {
            return true; // NOT FALSE is TRUE
        }
        return true; // Conservative for other NOT cases
    }
}
