package sequence

func Missing(watermark, incoming int64) []int64 {
	if incoming <= watermark+1 {
		return nil
	}
	result := make([]int64, 0, incoming-watermark-1)
	for value := watermark + 1; value < incoming; value++ {
		result = append(result, value)
	}
	return result
}
