package queuekafka

import "testing"

func TestSplitBrokers(t *testing.T) {
	got := splitBrokers("a:9092, b:9092 ,,c:9092")
	want := []string{"a:9092", "b:9092", "c:9092"}
	if len(got) != len(want) {
		t.Fatalf("splitBrokers = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
