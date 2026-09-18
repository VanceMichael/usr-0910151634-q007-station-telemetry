package sequence

import (
	"reflect"
	"testing"
)

func TestRanges(t *testing.T) {
	cases := []struct {
		name string
		in   []int64
		want []Range
	}{
		{"空", nil, nil},
		{"单个", []int64{7}, []Range{{7, 7}}},
		{"连续合并", []int64{4, 5, 6}, []Range{{4, 6}}},
		{"相邻区间", []int64{4, 5, 9}, []Range{{4, 5}, {9, 9}}},
		{"多段", []int64{4, 5, 9, 11, 12, 13}, []Range{{4, 5}, {9, 9}, {11, 13}}},
		{"乱序不合并跨段", []int64{11, 4, 12, 5, 9}, []Range{{11, 11}, {4, 4}, {12, 12}, {5, 5}, {9, 9}}},
	}
	for _, tc := range cases {
		if got := Ranges(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}
}
