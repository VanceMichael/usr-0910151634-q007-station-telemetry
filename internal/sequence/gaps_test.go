package sequence

import "testing"

func TestMissing(t *testing.T) {
	got := Missing(3, 6)
	if len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("缺口错误: %v", got)
	}
	if Missing(6, 4) != nil {
		t.Fatal("迟到序号不应制造缺口")
	}
}
