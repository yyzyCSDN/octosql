package nodes

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
)

// Row layouts used throughout the tests:
// left:  [symbol string, business time, qty int]      (3 fields)
// right: [symbol string, business time, price int]    (3 fields)
// output: the two concatenated, 6 fields.

func asofTestNode() *AsofJoin {
	return NewAsofJoin(
		nil, nil,
		3, 3,
		[]Expression{NewVariable(0, 0)},
		[]Expression{NewVariable(0, 0)},
		NewVariable(0, 1),
		NewVariable(0, 1),
	)
}

type scriptedEvent struct {
	record    *Record
	watermark *time.Time
}

type scriptedSource struct {
	name   string
	events <-chan scriptedEvent
}

func (s *scriptedSource) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for event := range s.events {
		if event.record != nil {
			if err := produce(ProduceFromExecutionContext(ctx), *event.record); err != nil {
				return fmt.Errorf("%s produce: %w", s.name, err)
			}
		} else {
			if err := metaSend(ProduceFromExecutionContext(ctx), MetadataMessage{
				Type:      MetadataMessageTypeWatermark,
				Watermark: *event.watermark,
			}); err != nil {
				return fmt.Errorf("%s meta: %w", s.name, err)
			}
		}
	}
	return nil
}

type outputEvent struct {
	record    Record
	watermark *time.Time
}

type asofTestHarness struct {
	t      *testing.T
	node   *AsofJoin
	left   chan scriptedEvent
	right  chan scriptedEvent
	output chan outputEvent
	done   chan error
}

func newAsofTestHarness(t *testing.T) *asofTestHarness {
	left := make(chan scriptedEvent)
	right := make(chan scriptedEvent)
	output := make(chan outputEvent, 1000)
	node := asofTestNode()
	node.left = &scriptedSource{name: "left", events: left}
	node.right = &scriptedSource{name: "right", events: right}

	h := &asofTestHarness{
		t:      t,
		node:   node,
		left:   left,
		right:  right,
		output: output,
		done:   make(chan error, 1),
	}

	go func() {
		h.done <- node.Run(
			ExecutionContext{},
			func(ctx ProduceContext, record Record) error {
				output <- outputEvent{record: record}
				return nil
			},
			func(ctx ProduceContext, msg MetadataMessage) error {
				wm := msg.Watermark
				output <- outputEvent{watermark: &wm}
				return nil
			},
		)
	}()

	return h
}

func (h *asofTestHarness) sendLeft(rec Record) {
	h.left <- scriptedEvent{record: &rec}
}

func (h *asofTestHarness) sendRight(rec Record) {
	h.right <- scriptedEvent{record: &rec}
}

func (h *asofTestHarness) sendLeftWatermark(t time.Time) {
	h.left <- scriptedEvent{watermark: &t}
}

func (h *asofTestHarness) sendRightWatermark(t time.Time) {
	h.right <- scriptedEvent{watermark: &t}
}

// barrier makes sure all records sent before it on both sides are processed before it
// returns, by advancing both watermarks together and waiting for the output marker.
func (h *asofTestHarness) barrier(sec int64) {
	h.sendLeftWatermark(wmTime(sec))
	h.sendRightWatermark(wmTime(sec))
	h.expectEvents(fmt.Sprintf("W %d", sec))
}

func (h *asofTestHarness) closeLeft() {
	close(h.left)
}

func (h *asofTestHarness) closeRight() {
	close(h.right)
}

// assertQuiescent asserts no more events are produced right now.
func (h *asofTestHarness) assertQuiescent() {
	select {
	case event := <-h.output:
		h.t.Fatalf("unexpected output event: %s", formatOutputEvent(event))
	case <-time.After(50 * time.Millisecond):
	}
}

func (h *asofTestHarness) waitDone() {
	if err := <-h.done; err != nil {
		h.t.Fatalf("asof join run failed: %v", err)
	}
}

func leftRow(symbol string, ts string, qty int64) []octosql.Value {
	return []octosql.Value{
		octosql.NewString(symbol),
		octosql.NewTime(parseTestTime(ts)),
		octosql.NewInt(qty),
	}
}

func rightRow(symbol string, ts string, price int64) []octosql.Value {
	return []octosql.Value{
		octosql.NewString(symbol),
		octosql.NewTime(parseTestTime(ts)),
		octosql.NewInt(price),
	}
}

func nullTimeLeftRow(symbol string, qty int64) []octosql.Value {
	return []octosql.Value{
		octosql.NewString(symbol),
		octosql.NewNull(),
		octosql.NewInt(qty),
	}
}

func nullTimeRightRow(symbol string, price int64) []octosql.Value {
	return []octosql.Value{
		octosql.NewString(symbol),
		octosql.NewNull(),
		octosql.NewInt(price),
	}
}

func parseTestTime(v string) time.Time {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		panic(err)
	}
	return t
}

func wmTime(sec int64) time.Time {
	return time.Unix(sec, 0)
}

func formatRow(values []octosql.Value) string {
	parts := make([]string, len(values))
	for i := range values {
		parts[i] = values[i].String()
	}
	return strings.Join(parts, ", ")
}

func formatOutputEvent(event outputEvent) string {
	if event.watermark != nil {
		return "WATERMARK " + event.watermark.String()
	}
	prefix := "+"
	if event.record.Retraction {
		prefix = "-"
	}
	return prefix + " " + formatRow(event.record.Values)
}

// expectEvents asserts the next outputs are exactly the given descriptions:
// entries prefixed with "+"/"-" are records, entries prefixed with "W" are watermarks.
func (h *asofTestHarness) expectEvents(expected ...string) {
	for i := range expected {
		select {
		case event := <-h.output:
			got := formatOutputEvent(event)
			var want string
			if strings.HasPrefix(expected[i], "W ") {
				wm := wmTime(parseWMSec(expected[i][2:]))
				want = "WATERMARK " + wm.String()
			} else {
				want = expected[i]
			}
			if got != want {
				h.t.Fatalf("output event %d mismatch:\nwant: %s\ngot:  %s", i, want, got)
			}
		case <-time.After(5 * time.Second):
			var pending []string
			for {
				select {
				case event := <-h.output:
					pending = append(pending, formatOutputEvent(event))
				default:
					h.t.Fatalf("timed out waiting for output event %d: %s\npending: %v", i, expected[i], pending)
				}
			}
		}
	}
}

func parseWMSec(s string) int64 {
	var sec int64
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &sec); err != nil {
		panic(err)
	}
	return sec
}

// finalMultiset folds the whole output event stream into the resulting row multiset.
func multiset(events []outputEvent) map[string]int {
	counts := map[string]int{}
	for _, event := range events {
		if event.watermark != nil {
			continue
		}
		key := formatRow(event.record.Values)
		if event.record.Retraction {
			counts[key]--
		} else {
			counts[key]++
		}
		if counts[key] == 0 {
			delete(counts, key)
		}
	}
	return counts
}

func expectMultiset(t *testing.T, events []outputEvent, expected map[string]int) {
	t.Helper()
	got := multiset(events)
	if len(got) != len(expected) {
		t.Fatalf("multiset size mismatch:\nwant: %v\ngot:  %v", expected, got)
	}
	for k, v := range expected {
		if got[k] != v {
			t.Fatalf("multiset mismatch for %q:\nwant: %v\ngot:  %v", k, expected, got)
		}
	}
}

func ins(values []octosql.Value) Record {
	return NewRecord(values, false, time.Time{})
}

func insET(values []octosql.Value, et time.Time) Record {
	return NewRecord(values, false, et)
}

func ret(values []octosql.Value) Record {
	return NewRecord(values, true, time.Time{})
}

func retET(values []octosql.Value, et time.Time) Record {
	return NewRecord(values, true, et)
}

func pair(left, right []octosql.Value) []octosql.Value {
	out := make([]octosql.Value, 6)
	copy(out, left)
	if right != nil {
		copy(out[3:], right)
	} else {
		out[3] = octosql.NewNull()
		out[4] = octosql.NewNull()
		out[5] = octosql.NewNull()
	}
	return out
}

func TestAsofJoinBasicFinite(t *testing.T) {
	h := newAsofTestHarness(t)

	h.sendRight(ins(rightRow("A", "2024-01-01T00:00:05Z", 100)))
	h.sendRight(ins(rightRow("A", "2024-01-01T00:00:15Z", 101)))
	h.closeRight()
	h.sendLeft(ins(leftRow("A", "2024-01-01T00:00:10Z", 1)))
	h.sendLeft(ins(leftRow("A", "2024-01-01T00:00:20Z", 2)))
	h.sendLeft(ins(leftRow("B", "2024-01-01T00:00:10Z", 3)))
	h.closeLeft()

	h.waitDone()

	// Drain everything.
	close(h.output)
	var events []outputEvent
	for event := range h.output {
		events = append(events, event)
	}

	// With finite inputs the intermediate sequence depends on scheduling, but the final
	// multiset is deterministic: the asof join result of the two complete snapshots.
	expectMultiset(t, events, map[string]int{
		formatRow(pair(leftRow("A", "2024-01-01T00:00:10Z", 1), rightRow("A", "2024-01-01T00:00:05Z", 100))): 1,
		formatRow(pair(leftRow("A", "2024-01-01T00:00:20Z", 2), rightRow("A", "2024-01-01T00:00:15Z", 101))): 1,
		formatRow(pair(leftRow("B", "2024-01-01T00:00:10Z", 3), nil)):                                        1,
	})
}

func TestAsofJoinRightContinuouslyCorrects(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)
	r8 := rightRow("A", "2024-01-01T00:00:08Z", 101)

	h.sendLeft(ins(l1))
	h.expectEvents("+ " + formatRow(pair(l1, nil)))
	h.assertQuiescent()

	h.sendRight(ins(r5))
	h.expectEvents(
		"- "+formatRow(pair(l1, nil)),
		"+ "+formatRow(pair(l1, r5)),
	)

	h.sendRight(ins(r8))
	h.expectEvents(
		"- "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, r8)),
	)

	// A quote strictly after the order time doesn't change anything.
	rLater := rightRow("A", "2024-01-01T00:00:20Z", 999)
	h.sendRight(ins(rLater))
	h.assertQuiescent()

	// An older quote doesn't change anything either.
	rOlder := rightRow("A", "2024-01-01T00:00:01Z", 1)
	h.sendRight(ins(rOlder))
	h.assertQuiescent()

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinRightRetractFallsBack(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)
	r8 := rightRow("A", "2024-01-01T00:00:08Z", 101)

	h.sendRight(ins(r5))
	h.sendRight(ins(r8))
	h.barrier(1)

	h.sendLeft(ins(l1))
	h.expectEvents("+ " + formatRow(pair(l1, r8)))

	// Retract the winning quote: fall back to the older one.
	h.sendRight(ret(r8))
	h.expectEvents(
		"- "+formatRow(pair(l1, r8)),
		"+ "+formatRow(pair(l1, r5)),
	)

	// Retract the last quote: fall back to NULL.
	h.sendRight(ret(r5))
	h.expectEvents(
		"- "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, nil)),
	)

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinDuplicateWinnerCopies(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)

	h.sendRight(ins(r5))
	h.sendRight(ins(r5)) // identical copy: only maintains candidate existence
	h.barrier(1)
	h.sendLeft(ins(l1))
	h.expectEvents("+ " + formatRow(pair(l1, r5))) // a single pairing, not a cartesian product

	// One copy retracted, one remains: no fallback.
	h.sendRight(ret(r5))
	h.assertQuiescent()

	// Last copy retracted: fall back to NULL.
	h.sendRight(ret(r5))
	h.expectEvents(
		"- "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, nil)),
	)

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinLeftDuplicatesAndRetract(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)

	h.sendRight(ins(r5))
	h.barrier(1)
	h.sendLeft(ins(l1))
	h.sendLeft(ins(l1)) // identical left copy: both copies must be preserved
	h.expectEvents(
		"+ "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, r5)),
	)

	h.sendLeft(ret(l1)) // retracting one copy removes only one pairing
	h.expectEvents("- " + formatRow(pair(l1, r5)))
	h.assertQuiescent()

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinTiePicksLexicographicallySmallest(t *testing.T) {
	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r1 := rightRow("A", "2024-01-01T00:00:05Z", 1)
	r2 := rightRow("A", "2024-01-01T00:00:05Z", 2)

	// Insert in both orders across two runs to make sure the result doesn't depend on arrival order.
	for _, order := range [][][]octosql.Value{{r2, r1}, {r1, r2}} {
		h := newAsofTestHarness(t)
		h.sendRight(ins(order[0]))
		h.sendRight(ins(order[1]))
		h.barrier(1)
		h.sendLeft(ins(l1))
		h.expectEvents("+ " + formatRow(pair(l1, r1)))
		h.closeLeft()
		h.closeRight()
		h.waitDone()
	}
}

func TestAsofJoinRightEndsLeftKeepsMatching(t *testing.T) {
	h := newAsofTestHarness(t)

	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)
	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	l2 := leftRow("A", "2024-01-01T00:00:20Z", 2)

	// Right side ends completely before the left stream continues. A barrier guarantees
	// its rows are processed before the side is closed.
	h.sendRight(ins(r5))
	h.barrier(1)
	h.closeRight()

	// Left catches up and ends later: new left rows keep matching the frozen right
	// side, with left-side watermarks forwarded once the right side has ended.
	h.sendLeft(insET(l1, wmTime(3)))
	h.sendLeftWatermark(wmTime(4))
	h.expectEvents(
		"+ "+formatRow(pair(l1, r5)),
		"W 4",
	)

	h.sendLeft(insET(l2, wmTime(5)))
	h.sendLeftWatermark(wmTime(6))
	h.expectEvents(
		"+ "+formatRow(pair(l2, r5)),
		"W 6",
	)

	h.closeLeft()
	h.waitDone()
}

func TestAsofJoinWatermarkedRightRetractionFallback(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:10:00Z", 1)
	r5 := rightRow("A", "2024-01-01T00:05:00Z", 100)
	r8 := rightRow("A", "2024-01-01T00:08:00Z", 101)

	// Left announces watermark 2 before it has any records.
	h.sendLeftWatermark(wmTime(2))
	h.assertQuiescent()

	// Right quote with event time 1 and watermark 2: it's released, there's no order yet.
	h.sendRight(insET(r5, wmTime(1)))
	h.sendRightWatermark(wmTime(2))
	h.expectEvents("W 2")

	// The order (event time 3) arrives with left watermark 4, but the right is still at
	// 2, so neither the record nor watermark 4 is published yet.
	h.sendLeft(insET(l1, wmTime(3)))
	h.sendLeftWatermark(wmTime(4))
	h.assertQuiescent()

	// Right retraction with event time 5 first releases the order at output watermark 4
	// (it pairs the quote); then left advances to 6, releasing the retraction which
	// falls back to NULL.
	h.sendRight(retET(r5, wmTime(5)))
	h.sendRightWatermark(wmTime(6))
	h.sendLeftWatermark(wmTime(6))
	h.expectEvents(
		"+ "+formatRow(pair(l1, r5)),
		"W 4",
		"- "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, nil)),
		"W 6",
	)

	// A new quote later corrects the NULL pairing at watermark 8.
	h.sendRight(insET(r8, wmTime(7)))
	h.sendRightWatermark(wmTime(8))
	h.assertQuiescent() // left is still at 6
	h.sendLeftWatermark(wmTime(8))
	h.expectEvents(
		"- "+formatRow(pair(l1, nil)),
		"+ "+formatRow(pair(l1, r8)),
		"W 8",
	)

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinNullKeysAndTimes(t *testing.T) {
	h := newAsofTestHarness(t)

	lnk := []octosql.Value{
		octosql.NewNull(),
		octosql.NewTime(parseTestTime("2024-01-01T00:00:10Z")),
		octosql.NewInt(1),
	}
	rnk := []octosql.Value{
		octosql.NewNull(),
		octosql.NewTime(parseTestTime("2024-01-01T00:00:05Z")),
		octosql.NewInt(100),
	}
	lnt := nullTimeLeftRow("A", 1)
	rnt := nullTimeRightRow("A", 100)
	l1 := leftRow("A", "2024-01-01T00:00:10Z", 2)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)

	h.sendLeft(ins(lnk))
	h.expectEvents("+ " + formatRow(pair(lnk, nil)))
	h.sendRight(ins(rnk)) // null key on right: never a candidate
	h.assertQuiescent()

	h.sendLeft(ins(lnt))
	h.expectEvents("+ " + formatRow(pair(lnt, nil)))
	h.sendRight(ins(rnt)) // null business time on right: never a candidate
	h.assertQuiescent()

	// A valid quote must not pair the unmatchable left rows.
	h.sendRight(ins(r5))
	h.assertQuiescent()

	// But it pairs a normal left row.
	h.sendLeft(ins(l1))
	h.expectEvents("+ " + formatRow(pair(l1, r5)))

	// Retracting unmatchable left rows just reduces counts.
	h.sendLeft(ret(lnk))
	h.expectEvents("- " + formatRow(pair(lnk, nil)))
	h.sendLeft(ret(lnt))
	h.expectEvents("- " + formatRow(pair(lnt, nil)))

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinWatermarkCoordination(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:10:00Z", 1)
	r5 := rightRow("A", "2024-01-01T00:05:00Z", 100)

	// Left record with event time 1. Left races ahead with watermark 5,
	// but the right side is still at zero: nothing can be published.
	h.sendLeft(insET(l1, wmTime(1)))
	h.sendLeftWatermark(wmTime(5))
	h.assertQuiescent()

	// Right catches up: the left NULL pairing is released with the watermark.
	h.sendRightWatermark(wmTime(5))
	h.expectEvents(
		"+ "+formatRow(pair(l1, nil)),
		"W 5",
	)

	// Right quote with event time 6; right advances to 10, left still at 5: nothing new.
	h.sendRight(insET(r5, wmTime(6)))
	h.sendRightWatermark(wmTime(10))
	h.assertQuiescent()

	// Left advances: the correction happens before the watermark 10 marker.
	h.sendLeftWatermark(wmTime(10))
	h.expectEvents(
		"- "+formatRow(pair(l1, nil)),
		"+ "+formatRow(pair(l1, r5)),
		"W 10",
	)

	h.closeLeft()
	h.closeRight()
	h.waitDone()
}

func TestAsofJoinLeftEndsRightKeepsCorrecting(t *testing.T) {
	h := newAsofTestHarness(t)

	l1 := leftRow("A", "2024-01-01T00:00:10Z", 1)
	r5 := rightRow("A", "2024-01-01T00:00:05Z", 100)
	r8 := rightRow("A", "2024-01-01T00:00:08Z", 101)

	h.sendLeft(insET(l1, wmTime(1)))
	h.sendLeftWatermark(wmTime(2))
	h.assertQuiescent() // right hasn't caught up yet

	h.sendRightWatermark(wmTime(2))
	h.expectEvents(
		"+ "+formatRow(pair(l1, nil)),
		"W 2",
	)

	h.closeLeft()

	// The right stream continues and must still correct the finished left side.
	h.sendRight(insET(r5, wmTime(3)))
	h.sendRight(insET(r8, wmTime(4)))
	h.sendRightWatermark(wmTime(5))
	h.expectEvents(
		"- "+formatRow(pair(l1, nil)),
		"+ "+formatRow(pair(l1, r5)),
		"- "+formatRow(pair(l1, r5)),
		"+ "+formatRow(pair(l1, r8)),
		"W 5",
	)

	// Retraction on the ongoing right side falls back after the left side ended.
	h.sendRight(retET(r8, wmTime(6)))
	h.sendRightWatermark(wmTime(7))
	h.expectEvents(
		"- "+formatRow(pair(l1, r8)),
		"+ "+formatRow(pair(l1, r5)),
		"W 7",
	)

	h.closeRight()
	h.waitDone()
}
