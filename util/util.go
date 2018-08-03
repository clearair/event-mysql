package util

func CompareString(a string, b string) int {
	lena := len(a)
	lenb := len(b)

	if lena == lenb && a == b {
		return 0
	}

	if lena > lenb {
		return 1
	}

	if lena < lenb {
		return -1
	}

	if a > b {
		return 1
	}

	if a < b {
		return -1
	}

	return 0
}