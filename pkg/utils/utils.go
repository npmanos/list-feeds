package utils

func Map[T, U any](collection []T, fn func(T) (U, error)) ([]U, error) {
	result := make([]U, len(collection))
	for i, item := range collection {
		output, err := fn(item)
		if err != nil {
			return nil, err
		}
		result[i] = output
	}

	return result, nil
}
