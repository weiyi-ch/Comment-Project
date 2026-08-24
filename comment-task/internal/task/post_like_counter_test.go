package task

import "testing"

func TestParseProcessingCounterBatchValue(t *testing.T) {
	postID, delta, err := parseProcessingCounterBatchValue("1001:-3")
	if err != nil {
		t.Fatal(err)
	}
	if postID != 1001 || delta != -3 {
		t.Fatalf("unexpected parsed value, post_id=%d, delta=%d", postID, delta)
	}
}

func TestParseProcessingCounterBatchValueRejectsInvalidValue(t *testing.T) {
	cases := []string{
		"",
		"1001",
		"abc:1",
		"1001:0",
		"1001:abc",
	}
	for _, tc := range cases {
		if _, _, err := parseProcessingCounterBatchValue(tc); err == nil {
			t.Fatalf("value %q should be rejected", tc)
		}
	}
}
