package logical

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/optimizer"
	"github.com/cube2222/octosql/physical"
)

type StreamJoin struct {
	left, right Node
}

func NewStreamJoin(left, right Node) *StreamJoin {
	return &StreamJoin{
		left:  left,
		right: right,
	}
}

func (node *StreamJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env, logicalEnv)

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		// TODO: Duplicates should be handled well.
		rightMapping[k] = v
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:        append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
			TimeField:     left.Schema.TimeField,
			NoRetractions: left.Schema.NoRetractions && right.Schema.NoRetractions,
		},
		NodeType: physical.NodeTypeStreamJoin,
		StreamJoin: &physical.StreamJoin{
			Left:  left,
			Right: right,
		},
	}, rightMapping
}

type OuterJoin struct {
	left, right     Node
	predicate       Expression
	isLeft, isRight bool
}

func NewOuterJoin(left, right Node, predicate Expression, isLeft, isRight bool) *OuterJoin {
	return &OuterJoin{
		left:      left,
		right:     right,
		predicate: predicate,
		isLeft:    isLeft,
		isRight:   isRight,
	}
}

func (node *OuterJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env, logicalEnv)

	outMapping := rightMapping

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		// TODO: Duplicates should be handled well.
		outMapping[k] = v
	}

	predicate := node.predicate.Typecheck(ctx, env.WithRecordSchema(right.Schema).WithRecordSchema(left.Schema), logicalEnv.WithRecordUniqueVariableNames(outMapping))

	filterPredicates := predicate.SplitByAnd()
	var leftKey, rightKey []physical.Expression

	for i := range filterPredicates {
		if filterPredicates[i].ExpressionType != physical.ExpressionTypeFunctionCall || filterPredicates[i].FunctionCall.Name != "=" {
			panic(fmt.Errorf("outer join predicate must be a conjunction of equalities"))
		}
		firstPart := filterPredicates[i].FunctionCall.Arguments[0]
		secondPart := filterPredicates[i].FunctionCall.Arguments[1]
		firstPartVariables := firstPart.VariablesUsed()
		firstPartUsesLeftVariables := optimizer.UsesVariablesFromSchema(left.Schema, firstPartVariables)
		firstPartUsesRightVariables := optimizer.UsesVariablesFromSchema(right.Schema, firstPartVariables)
		secondPartVariables := secondPart.VariablesUsed()
		secondPartUsesLeftVariables := optimizer.UsesVariablesFromSchema(left.Schema, secondPartVariables)
		secondPartUsesRightVariables := optimizer.UsesVariablesFromSchema(right.Schema, secondPartVariables)

		if firstPartUsesLeftVariables && !firstPartUsesRightVariables &&
			!secondPartUsesLeftVariables && secondPartUsesRightVariables {
			leftKey = append(leftKey, firstPart)
			rightKey = append(rightKey, secondPart)
		} else if !firstPartUsesLeftVariables && firstPartUsesRightVariables &&
			secondPartUsesLeftVariables && !secondPartUsesRightVariables {
			rightKey = append(rightKey, firstPart)
			leftKey = append(leftKey, secondPart)
		} else {
			panic(fmt.Errorf("outer join predicate equality predicate left and right side must each reference only one of the input tables"))
		}
	}

	outSchemaFields := append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...)
	if node.isLeft {
		for i := len(left.Schema.Fields); i < len(outSchemaFields); i++ {
			outSchemaFields[i] = physical.SchemaField{
				Name: outSchemaFields[i].Name,
				Type: octosql.TypeSum(outSchemaFields[i].Type, octosql.Null),
			}
		}
	}
	if node.isRight {
		for i := 0; i < len(left.Schema.Fields); i++ {
			outSchemaFields[i] = physical.SchemaField{
				Name: outSchemaFields[i].Name,
				Type: octosql.TypeSum(outSchemaFields[i].Type, octosql.Null),
			}
		}
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:        outSchemaFields,
			TimeField:     left.Schema.TimeField,
			NoRetractions: left.Schema.NoRetractions && right.Schema.NoRetractions,
		},
		NodeType: physical.NodeTypeOuterJoin,
		OuterJoin: &physical.OuterJoin{
			Left:     left,
			Right:    right,
			LeftKey:  leftKey,
			RightKey: rightKey,
			IsLeft:   node.isLeft,
			IsRight:  node.isRight,
		},
	}, outMapping
}

type AsofJoin struct {
	left, right Node
	predicate   Expression
}

func NewAsofJoin(left, right Node, predicate Expression) *AsofJoin {
	return &AsofJoin{
		left:      left,
		right:     right,
		predicate: predicate,
	}
}

func (node *AsofJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env, logicalEnv)

	outMapping := rightMapping
	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		outMapping[k] = v
	}

	predicate := node.predicate.Typecheck(ctx, env.WithRecordSchema(right.Schema).WithRecordSchema(left.Schema), logicalEnv.WithRecordUniqueVariableNames(outMapping))

	filterPredicates := predicate.SplitByAnd()
	var leftKey, rightKey []physical.Expression
	var leftTime, rightTime *physical.Expression

	// classifyCrossSideExpression checks that the expression references variables
	// of exactly one join side and returns it together with which side that is.
	classifyCrossSideExpression := func(expr physical.Expression) (physical.Expression, bool, error) {
		variables := expr.VariablesUsed()
		usesLeft := optimizer.UsesVariablesFromSchema(left.Schema, variables)
		usesRight := optimizer.UsesVariablesFromSchema(right.Schema, variables)
		if usesLeft == usesRight {
			return physical.Expression{}, false, fmt.Errorf("asof join predicate parts must each reference exactly one of the joined tables")
		}
		return expr, usesLeft, nil
	}

	for i := range filterPredicates {
		pred := filterPredicates[i]
		if pred.ExpressionType != physical.ExpressionTypeFunctionCall {
			panic(fmt.Errorf("asof join ON clause must be a conjunction of cross-side equalities and exactly one right Time <= left Time comparison"))
		}
		switch pred.FunctionCall.Name {
		case "=":
			firstPart := pred.FunctionCall.Arguments[0]
			secondPart := pred.FunctionCall.Arguments[1]
			p1, firstIsLeft, err := classifyCrossSideExpression(firstPart)
			if err != nil {
				panic(err)
			}
			p2, secondIsLeft, err := classifyCrossSideExpression(secondPart)
			if err != nil {
				panic(err)
			}
			if firstIsLeft == secondIsLeft {
				panic(fmt.Errorf("asof join equality predicate sides must each reference only one of the input tables"))
			}
			if firstIsLeft {
				leftKey = append(leftKey, p1)
				rightKey = append(rightKey, p2)
			} else {
				rightKey = append(rightKey, p1)
				leftKey = append(leftKey, p2)
			}
		case "<=":
			if leftTime != nil {
				panic(fmt.Errorf("asof join ON clause may contain at most one time comparison (right Time <= left Time)"))
			}
			leftOperand, firstIsLeft, err := classifyCrossSideExpression(pred.FunctionCall.Arguments[0])
			if err != nil {
				panic(err)
			}
			rightOperand, secondIsLeft, err := classifyCrossSideExpression(pred.FunctionCall.Arguments[1])
			if err != nil {
				panic(err)
			}
			// The only supported shape is: right Time <= left Time.
			if firstIsLeft || !secondIsLeft {
				panic(fmt.Errorf("asof join time comparison must be of the form: right_time_column <= left_time_column"))
			}
			if octosql.NonNullable(rightOperand.Type).TypeID != octosql.TypeIDTime || octosql.NonNullable(leftOperand.Type).TypeID != octosql.TypeIDTime {
				panic(fmt.Errorf("asof join time comparison columns must both be of type Time, got %s <= %s", leftOperand.Type.String(), rightOperand.Type.String()))
			}
			lt := rightOperand
			rt := leftOperand
			leftTime = &lt
			rightTime = &rt
		default:
			panic(fmt.Errorf("asof join ON clause only supports '=' equality conditions and one '<=' time comparison, got function %q", pred.FunctionCall.Name))
		}
	}

	if len(leftKey) == 0 {
		panic(fmt.Errorf("asof join ON clause must contain at least one cross-side equality condition"))
	}
	if leftTime == nil {
		panic(fmt.Errorf("asof join ON clause must contain exactly one time comparison of the form: right_time_column <= left_time_column"))
	}

	outSchemaFields := append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...)
	for i := len(left.Schema.Fields); i < len(outSchemaFields); i++ {
		outSchemaFields[i] = physical.SchemaField{
			Name: outSchemaFields[i].Name,
			Type: octosql.TypeSum(outSchemaFields[i].Type, octosql.Null),
		}
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:    outSchemaFields,
			TimeField: left.Schema.TimeField,
			// ASOF join outputs can be retracted even when both inputs are insert-only,
			// because later right rows correct earlier pairings.
			NoRetractions: false,
		},
		NodeType: physical.NodeTypeAsofJoin,
		AsofJoin: &physical.AsofJoin{
			Left:      left,
			Right:     right,
			LeftKey:   leftKey,
			RightKey:  rightKey,
			LeftTime:  *leftTime,
			RightTime: *rightTime,
		},
	}, outMapping
}

type LookupJoin struct {
	left, right Node
}

func NewLookupJoin(left, right Node) *LookupJoin {
	return &LookupJoin{
		left:  left,
		right: right,
	}
}

func (node *LookupJoin) Typecheck(ctx context.Context, env physical.Environment, logicalEnv Environment) (physical.Node, map[string]string) {
	// TODO: This should currently fail when the joined stream has a time field / is endless.
	left, leftMapping := node.left.Typecheck(ctx, env, logicalEnv)
	right, rightMapping := node.right.Typecheck(ctx, env.WithRecordSchema(left.Schema), logicalEnv.WithRecordUniqueVariableNames(leftMapping))

	for k, v := range leftMapping {
		// Put all mapped variables into one map.
		// Left mapping takes precedence. Duplicates get overwritten.
		rightMapping[k] = v
	}

	return physical.Node{
		Schema: physical.Schema{
			Fields:    append(left.Schema.Fields[:len(left.Schema.Fields):len(left.Schema.Fields)], right.Schema.Fields[:len(right.Schema.Fields):len(right.Schema.Fields)]...),
			TimeField: left.Schema.TimeField,
		},
		NodeType: physical.NodeTypeLookupJoin,
		LookupJoin: &physical.LookupJoin{
			Source: left,
			Joined: right,
		},
	}, rightMapping
}
