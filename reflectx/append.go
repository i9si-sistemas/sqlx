package reflectx

// A copying append that creates a new slice each time.
func apnd(is []int, i int) []int {
	x := make([]int, len(is)+1)
	copy(x, is)
	x[len(x)-1] = i
	return x
}