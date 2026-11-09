package nodes

import (
	"fmt"
	"time"

	tbtree "github.com/tidwall/btree"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// AsofJoin is an ASOF LEFT JOIN.
//
// For every live copy of a left row it picks the right row with the same equality keys
// whose business time is the largest one not greater than the left row's business time.
// If multiple right rows tie on business time, the lexicographically smallest row
// (in right input column order, using the usual OctoSQL value ordering) is chosen.
// If no such right row exists, the right columns are NULL.
//
// It is a maintaining, correcting join: later right rows retract old pairings and
// establish new ones, retracting the last copy of the winning right row falls back to
// the next candidate or NULL, and retracting a left row copy only removes that one
// output copy.
type AsofJoin struct {
	left, right                     Node
	leftFieldCount, rightFieldCount int
	keyExprsLeft, keyExprsRight     []Expression
	timeExprLeft, timeExprRight     Expression
}

func NewAsofJoin(left, right Node, leftFieldCount, rightFieldCount int, keyExprsLeft, keyExprsRight []Expression, timeExprLeft, timeExprRight Expression) *AsofJoin {
	return &AsofJoin{
		left:            left,
		right:           right,
		leftFieldCount:  leftFieldCount,
		rightFieldCount: rightFieldCount,
		keyExprsLeft:    keyExprsLeft,
		keyExprsRight:   keyExprsRight,
		timeExprLeft:    timeExprLeft,
		timeExprRight:   timeExprRight,
	}
}

type asofLeftGroup struct {
	GroupKey
	rows *tbtree.Generic[*asofLeftRow]
}

type asofLeftRow struct {
	// Full left row values.
	GroupKey
	// Business comparison time of this row.
	businessTime octosql.Value
	// EventTimes of live copies of this exact row; one entry per copy.
	EventTimes []time.Time
	// Full right row values this row is currently paired with; nil means the NULL pairing.
	winner GroupKey
	// Business time of the currently paired right candidate.
	winnerTime octosql.Value
}

type asofRightGroup struct {
	GroupKey
	candidates *tbtree.Generic[*asofRightCandidate]
}

type asofRightCandidate struct {
	// Business time of the right row.
	businessTime octosql.Value
	// Full right row values.
	GroupKey
	// EventTimes of live copies of this exact row; one entry per copy.
	EventTimes []time.Time
}

func asofNewLeftGroupTree() *tbtree.Generic[*asofLeftGroup] {
	return tbtree.NewGenericOptions[*asofLeftGroup](func(a, b *asofLeftGroup) bool {
		return CompareValueSlices(a.GroupKey, b.GroupKey)
	}, tbtree.Options{NoLocks: true})
}

func asofNewRightGroupTree() *tbtree.Generic[*asofRightGroup] {
	return tbtree.NewGenericOptions[*asofRightGroup](func(a, b *asofRightGroup) bool {
		return CompareValueSlices(a.GroupKey, b.GroupKey)
	}, tbtree.Options{NoLocks: true})
}

func asofNewLeftRowTree() *tbtree.Generic[*asofLeftRow] {
	return tbtree.NewGenericOptions[*asofLeftRow](func(a, b *asofLeftRow) bool {
		return CompareValueSlices(a.GroupKey, b.GroupKey)
	}, tbtree.Options{NoLocks: true})
}

// asofNewCandidateTree orders candidates by business time ascending, and for ties
// on business time by full row values DESCENDING, so that the largest item with a
// business time not greater than the left business time is the winner: it has the
// largest time, and among ties the lexicographically smallest row.
func asofNewCandidateTree() *tbtree.Generic[*asofRightCandidate] {
	return tbtree.NewGenericOptions[*asofRightCandidate](func(a, b *asofRightCandidate) bool {
		if comp := a.businessTime.Compare(b.businessTime); comp != 0 {
			return comp == -1
		}
		return CompareValueSlices(b.GroupKey, a.GroupKey)
	}, tbtree.Options{NoLocks: true})
}

func minTime(times []time.Time) time.Time {
	min := times[0]
	for i := 1; i < len(times); i++ {
		if times[i].Before(min) {
			min = times[i]
		}
	}
	return min
}

func sameRowValues(a, b GroupKey) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return CompareValueSlices(a, b) == false && CompareValueSlices(b, a) == false
}

func (s *AsofJoin) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	type chanMessage struct {
		metadata        bool
		metadataMessage MetadataMessage
		record          Record
		err             error
	}

	leftMessages := make(chan chanMessage, 10000)
	rightMessages := make(chan chanMessage, 10000)

	go func() {
		if err := s.left.Run(ctx, func(produceCtx ProduceContext, record Record) error {
			leftMessages <- chanMessage{
				metadata: false,
				record:   record,
			}

			return nil
		}, func(ctx ProduceContext, msg MetadataMessage) error {
			leftMessages <- chanMessage{
				metadata:        true,
				metadataMessage: msg,
			}

			return nil
		}); err != nil {
			leftMessages <- chanMessage{
				err: fmt.Errorf("couldn't run left asof join source: %w", err),
			}
		}

		close(leftMessages)
	}()

	go func() {
		if err := s.right.Run(ctx, func(produceCtx ProduceContext, record Record) error {
			rightMessages <- chanMessage{
				metadata: false,
				record:   record,
			}

			return nil
		}, func(ctx ProduceContext, msg MetadataMessage) error {
			rightMessages <- chanMessage{
				metadata:        true,
				metadataMessage: msg,
			}

			return nil
		}); err != nil {
			rightMessages <- chanMessage{
				err: fmt.Errorf("couldn't run right asof join source: %w", err),
			}
		}

		close(rightMessages)
	}()

	// Both state trees are kept for the whole run, even after one side ends:
	// a finished left side can still be corrected by ongoing right input, and
	// rows are never evicted based on business time.
	leftRecords := asofNewLeftGroupTree()
	rightRecords := asofNewRightGroupTree()
	// Left rows which can never match (null equality key, or null business comparison
	// time) still get the NULL pairing and are kept as a multiset for retractions.
	leftUnmatchable := asofNewLeftRowTree()

	var leftDone bool

	var leftWatermark, rightWatermark, minWatermark time.Time
	// lastForwardedWatermark tracks the greatest output watermark already sent, so that
	// a watermark consumed right around the moment when the other side ends isn't lost.
	var lastForwardedWatermark time.Time

	leftRecordBuffer := NewRecordEventTimeBuffer()
	rightRecordBuffer := NewRecordEventTimeBuffer()

	processRecordsUpTo := func(ctx ExecutionContext, watermark time.Time) error {
		if err := leftRecordBuffer.Emit(watermark, func(record Record) error {
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, leftUnmatchable, true, record); err != nil {
				return fmt.Errorf("couldn't process record from left: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}

		if err := rightRecordBuffer.Emit(watermark, func(record Record) error {
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, leftUnmatchable, false, record); err != nil {
				return fmt.Errorf("couldn't process record from right: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}

		return nil
	}

receiveLoop:
	for {
		select {
		case msg, ok := <-leftMessages:
			if !ok {
				leftDone = true
				break receiveLoop
			}
			if msg.err != nil {
				return msg.err
			}
			if msg.metadata {
				leftWatermark = msg.metadataMessage.Watermark

				var min time.Time
				if leftWatermark.After(rightWatermark) {
					min = rightWatermark
				} else {
					min = leftWatermark
				}
				if min.After(minWatermark) {
					minWatermark = min

					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
						return err
					}

					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
					lastForwardedWatermark = minWatermark
				}

				continue
			}
			if msg.record.EventTime.IsZero() {
				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, leftUnmatchable, true, msg.record); err != nil {
					return fmt.Errorf("couldn't process record from left: %w", err)
				}
			} else {
				leftRecordBuffer.AddRecord(msg.record)
			}

		case msg, ok := <-rightMessages:
			if !ok {
				leftDone = false
				break receiveLoop
			}
			if msg.err != nil {
				return msg.err
			}
			if msg.metadata {
				rightWatermark = msg.metadataMessage.Watermark

				var min time.Time
				if rightWatermark.After(leftWatermark) {
					min = leftWatermark
				} else {
					min = rightWatermark
				}
				if min.After(minWatermark) {
					minWatermark = min

					if err := processRecordsUpTo(ctx, minWatermark); err != nil {
						return err
					}

					if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
						Type:      MetadataMessageTypeWatermark,
						Watermark: minWatermark,
					}); err != nil {
						return fmt.Errorf("couldn't send metadata: %w", err)
					}
					lastForwardedWatermark = minWatermark
				}

				continue
			}
			if msg.record.EventTime.IsZero() {
				if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, leftUnmatchable, false, msg.record); err != nil {
					return fmt.Errorf("couldn't process record from right: %w", err)
				}
			} else {
				rightRecordBuffer.AddRecord(msg.record)
			}
		}
	}

	var openChannel chan chanMessage
	var myRecordBuffer *RecordEventTimeBuffer
	if !leftDone {
		openChannel = leftMessages
		myRecordBuffer = leftRecordBuffer
		minWatermark = leftWatermark
	} else {
		openChannel = rightMessages
		myRecordBuffer = rightRecordBuffer
		minWatermark = rightWatermark
	}

	// The ended side is complete, so from now on the output watermark is the remaining
	// side's watermark. It may have advanced already (its watermark message may even have
	// been consumed right before we observed the other side ending), so make sure the
	// transition watermark is published exactly once.
	if err := processRecordsUpTo(ctx, minWatermark); err != nil {
		return err
	}
	if minWatermark.After(lastForwardedWatermark) {
		if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
			Type:      MetadataMessageTypeWatermark,
			Watermark: minWatermark,
		}); err != nil {
			return fmt.Errorf("couldn't send metadata: %w", err)
		}
		lastForwardedWatermark = minWatermark
	}

	for msg := range openChannel {
		if msg.err != nil {
			return msg.err
		}
		if msg.metadata {
			if err := processRecordsUpTo(ctx, msg.metadataMessage.Watermark); err != nil {
				return err
			}

			if err := metaSend(ProduceFromExecutionContext(ctx), msg.metadataMessage); err != nil {
				return fmt.Errorf("couldn't send metadata: %w", err)
			}
			lastForwardedWatermark = msg.metadataMessage.Watermark
			continue
		}

		if msg.record.EventTime.IsZero() {
			if err := s.receiveRecord(ctx, produce, leftRecords, rightRecords, leftUnmatchable, !leftDone, msg.record); err != nil {
				return fmt.Errorf("couldn't process record: %w", err)
			}
		} else {
			myRecordBuffer.AddRecord(msg.record)
		}
	}

	// Flush everything at the end.
	if err := processRecordsUpTo(ctx, WatermarkMaxValue); err != nil {
		return err
	}

	return nil
}

func groupKeyHasNull(key GroupKey) bool {
	for i := range key {
		if key[i].TypeID == octosql.TypeIDNull {
			return true
		}
	}
	return false
}

// pickWinner returns the candidate that should pair with a left row at the given
// business time, or nil if there is no eligible candidate.
func pickWinner(group *asofRightGroup, leftBusinessTime octosql.Value) *asofRightCandidate {
	if group == nil {
		return nil
	}
	var winner *asofRightCandidate
	// Candidates are ordered (time asc, values desc on tie), so walking from the end,
	// the first candidate with time <= left time is the most recent one, and on a
	// time tie the lexicographically smallest row.
	group.candidates.Reverse(func(candidate *asofRightCandidate) bool {
		if candidate.businessTime.Compare(leftBusinessTime) > 0 {
			return true
		}
		winner = candidate
		return false
	})
	return winner
}

// emitPairing emits one output record. If candidate is nil, the right columns are NULL.
func (s *AsofJoin) emitPairing(ctx ExecutionContext, produce ProduceFn, leftValues []octosql.Value, leftEventTime time.Time, candidate *asofRightCandidate, retraction bool) error {
	outputValues := make([]octosql.Value, s.leftFieldCount+s.rightFieldCount)
	copy(outputValues, leftValues)

	eventTime := leftEventTime
	if candidate != nil {
		copy(outputValues[s.leftFieldCount:], candidate.GroupKey)
		if len(candidate.EventTimes) > 0 {
			if candidateEventTime := minTime(candidate.EventTimes); candidateEventTime.After(eventTime) {
				eventTime = candidateEventTime
			}
		}
	} else {
		for i := s.leftFieldCount; i < len(outputValues); i++ {
			outputValues[i] = octosql.NewNull()
		}
	}

	if err := produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, retraction, eventTime)); err != nil {
		return fmt.Errorf("couldn't produce: %w", err)
	}
	return nil
}

// emitPairingWithRightValues emits a pairing for a candidate that may already have been
// removed from the right state tree, in which case only its values are known and the
// left event time is used for the output record.
func (s *AsofJoin) emitPairingWithRightValues(ctx ExecutionContext, produce ProduceFn, leftValues []octosql.Value, leftEventTime time.Time, rightValues []octosql.Value, retraction bool) error {
	outputValues := make([]octosql.Value, s.leftFieldCount+s.rightFieldCount)
	copy(outputValues, leftValues)
	if rightValues != nil {
		copy(outputValues[s.leftFieldCount:], rightValues)
	} else {
		for i := s.leftFieldCount; i < len(outputValues); i++ {
			outputValues[i] = octosql.NewNull()
		}
	}

	if err := produce(ProduceFromExecutionContext(ctx), NewRecord(outputValues, retraction, leftEventTime)); err != nil {
		return fmt.Errorf("couldn't produce: %w", err)
	}
	return nil
}

func (s *AsofJoin) receiveRecord(ctx ExecutionContext, produce ProduceFn, leftRecords *tbtree.Generic[*asofLeftGroup], rightRecords *tbtree.Generic[*asofRightGroup], leftUnmatchable *tbtree.Generic[*asofLeftRow], amLeft bool, record Record) error {
	ctx = ctx.WithRecord(record)

	if amLeft {
		return s.receiveLeft(ctx, produce, leftRecords, rightRecords, leftUnmatchable, record)
	}
	return s.receiveRight(ctx, produce, leftRecords, rightRecords, record)
}

func (s *AsofJoin) receiveLeft(ctx ExecutionContext, produce ProduceFn, leftRecords *tbtree.Generic[*asofLeftGroup], rightRecords *tbtree.Generic[*asofRightGroup], leftUnmatchable *tbtree.Generic[*asofLeftRow], record Record) error {
	key := make(GroupKey, len(s.keyExprsLeft))
	for i, expr := range s.keyExprsLeft {
		value, err := expr.Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate %d asof join left key expression: %w", i, err)
		}
		key[i] = value
	}

	leftBusinessTime, err := s.timeExprLeft.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate asof join left time expression: %w", err)
	}

	// Null equality keys and null comparison times never match.
	if groupKeyHasNull(key) || leftBusinessTime.TypeID == octosql.TypeIDNull {
		row, _ := leftUnmatchable.Get(&asofLeftRow{GroupKey: record.Values})
		if !record.Retraction {
			if row == nil {
				row = &asofLeftRow{GroupKey: record.Values}
				leftUnmatchable.Set(row)
			}
			row.EventTimes = append(row.EventTimes, record.EventTime)
			if err := s.emitPairing(ctx, produce, record.Values, record.EventTime, nil, false); err != nil {
				return err
			}
		} else {
			if row == nil {
				return fmt.Errorf("received asof join left retraction for unmatchable row that wasn't inserted")
			}
			if err := s.emitPairing(ctx, produce, record.Values, record.EventTime, nil, true); err != nil {
				return err
			}
			row.EventTimes = row.EventTimes[1:]
			if len(row.EventTimes) == 0 {
				leftUnmatchable.Delete(row)
			}
		}
		return nil
	}

	group, _ := leftRecords.Get(&asofLeftGroup{GroupKey: key})
	rightGroup, _ := rightRecords.Get(&asofRightGroup{GroupKey: key})

	if !record.Retraction {
		if group == nil {
			group = &asofLeftGroup{GroupKey: key, rows: asofNewLeftRowTree()}
			leftRecords.Set(group)
		}
		candidate := pickWinner(rightGroup, leftBusinessTime)

		row, _ := group.rows.Get(&asofLeftRow{GroupKey: record.Values})
		if row == nil {
			var winner GroupKey
			var winnerTime octosql.Value
			if candidate != nil {
				winner = candidate.GroupKey
				winnerTime = candidate.businessTime
			}
			row = &asofLeftRow{
				GroupKey:     record.Values,
				businessTime: leftBusinessTime,
				winner:       winner,
				winnerTime:   winnerTime,
			}
			group.rows.Set(row)
		}
		row.EventTimes = append(row.EventTimes, record.EventTime)

		if err := s.emitPairing(ctx, produce, record.Values, record.EventTime, candidate, false); err != nil {
			return err
		}
		return nil
	}

	if group == nil {
		return fmt.Errorf("received asof join left retraction for key group that doesn't exist")
	}
	row, _ := group.rows.Get(&asofLeftRow{GroupKey: record.Values})
	if row == nil {
		return fmt.Errorf("received asof join left retraction for row that wasn't inserted")
	}

	var oldCandidate *asofRightCandidate
	if row.winner != nil && rightGroup != nil {
		oldCandidate, _ = rightGroup.candidates.Get(&asofRightCandidate{businessTime: row.winnerTime, GroupKey: row.winner})
	}
	if err := s.emitPairing(ctx, produce, record.Values, record.EventTime, oldCandidate, true); err != nil {
		return err
	}
	row.EventTimes = row.EventTimes[1:]
	if len(row.EventTimes) == 0 {
		group.rows.Delete(row)
	}
	if group.rows.Len() == 0 {
		leftRecords.Delete(group)
	}
	return nil
}

func (s *AsofJoin) receiveRight(ctx ExecutionContext, produce ProduceFn, leftRecords *tbtree.Generic[*asofLeftGroup], rightRecords *tbtree.Generic[*asofRightGroup], record Record) error {
	key := make(GroupKey, len(s.keyExprsRight))
	for i, expr := range s.keyExprsRight {
		value, err := expr.Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate %d asof join right key expression: %w", i, err)
		}
		key[i] = value
	}

	rightBusinessTime, err := s.timeExprRight.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("couldn't evaluate asof join right time expression: %w", err)
	}

	// Null equality keys or null comparison times can never be candidates.
	if groupKeyHasNull(key) || rightBusinessTime.TypeID == octosql.TypeIDNull {
		return nil
	}

	group, _ := rightRecords.Get(&asofRightGroup{GroupKey: key})
	if group == nil {
		group = &asofRightGroup{GroupKey: key, candidates: asofNewCandidateTree()}
		rightRecords.Set(group)
	}

	candidateKey := &asofRightCandidate{businessTime: rightBusinessTime, GroupKey: record.Values}
	candidate, _ := group.candidates.Get(candidateKey)
	if !record.Retraction {
		if candidate == nil {
			candidate = &asofRightCandidate{businessTime: rightBusinessTime, GroupKey: record.Values}
			group.candidates.Set(candidate)
		}
		candidate.EventTimes = append(candidate.EventTimes, record.EventTime)
	} else {
		if candidate == nil || len(candidate.EventTimes) == 0 {
			return fmt.Errorf("received asof join right retraction for candidate that wasn't inserted")
		}
		candidate.EventTimes = candidate.EventTimes[1:]
		if len(candidate.EventTimes) > 0 {
			// Copies of this candidate still exist: it stays the winner, no fallback.
			return nil
		}
		group.candidates.Delete(candidate)
		if group.candidates.Len() == 0 {
			rightRecords.Delete(group)
		}
	}

	// Correct all left rows in this key group whose winner changed.
	leftGroup, _ := leftRecords.Get(&asofLeftGroup{GroupKey: key})
	if leftGroup == nil {
		return nil
	}

	var outErr error
	leftGroup.rows.Scan(func(row *asofLeftRow) bool {
		newWinner := pickWinner(group, row.businessTime)

		var newWinnerKey GroupKey
		if newWinner != nil {
			newWinnerKey = newWinner.GroupKey
		}
		if sameRowValues(row.winner, newWinnerKey) {
			return true
		}

		// The old candidate might have just been deleted by this very retraction, in
		// which case it's no longer in the tree; emit the retraction with its stored
		// values and the left copy's event time.
		var oldCandidate *asofRightCandidate
		if row.winner != nil {
			oldCandidate, _ = group.candidates.Get(&asofRightCandidate{businessTime: row.winnerTime, GroupKey: row.winner})
		}

		for i := range row.EventTimes {
			leftEventTime := row.EventTimes[i]
			if oldCandidate != nil {
				if err := s.emitPairing(ctx, produce, row.GroupKey, leftEventTime, oldCandidate, true); err != nil {
					outErr = err
					return false
				}
			} else {
				if err := s.emitPairingWithRightValues(ctx, produce, row.GroupKey, leftEventTime, row.winner, true); err != nil {
					outErr = err
					return false
				}
			}

			if err := s.emitPairing(ctx, produce, row.GroupKey, leftEventTime, newWinner, false); err != nil {
				outErr = err
				return false
			}
		}

		row.winner = newWinnerKey
		if newWinner != nil {
			row.winnerTime = newWinner.businessTime
		} else {
			row.winnerTime = octosql.ZeroValue
		}
		return true
	})
	if outErr != nil {
		return outErr
	}
	return nil
}
