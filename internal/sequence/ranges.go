package sequence

// Range 表示一段连续的待补序号闭区间 [Start, End]。
type Range struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// Ranges 把离散且已升序去重的待补序号合并成连续区间。
// 例如 [4,5,9] => [{4 5} {9 9}]，交班接口据此给出“仍待补齐的序号区间”。
func Ranges(missing []int64) []Range {
	if len(missing) == 0 {
		return nil
	}
	result := make([]Range, 0, len(missing))
	start := missing[0]
	end := missing[0]
	for _, value := range missing[1:] {
		if value == end {
			continue
		}
		if value == end+1 {
			end = value
			continue
		}
		result = append(result, Range{Start: start, End: end})
		start, end = value, value
	}
	result = append(result, Range{Start: start, End: end})
	return result
}
