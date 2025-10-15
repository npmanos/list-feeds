package utils

import (
	"fmt"
	"iter"
	"strings"
)

func Map[T, U any](slice []T, fn func(T) (U, error)) ([]U, error) {
	result := make([]U, len(slice))
	for i, item := range slice {
		output, err := fn(item)
		if err != nil {
			return nil, err
		}
		result[i] = output
	}

	return result, nil
}

func Prepend[E any](slice []E, elem E) ([]E) {
	var zero E
	slice = append(slice, zero)
	copy(slice[1:], slice)
	slice[0] = elem

	return slice
}

func BuildAtURI(did string, collection string, rKey string) string {
	return fmt.Sprintf("at://%s/%s/%s", did, collection, rKey)
}

func ExtractDID(atURI string) (string, error) {
	did, _ := strings.CutPrefix(atURI, "at://")
	did = strings.Split(did, "/")[0]

	if !strings.HasPrefix(did, "did:") {
		return "", fmt.Errorf("couldn't find a valid DID in list URI %s", atURI)
	}

	return did, nil
}

type stack[E any] struct {
	s []E
}

func NewStack[E any]() *stack[E] {
	return &stack[E]{s: make([]E, 0)}
}

func (s *stack[E]) Push(e E) {
	s.s = append(s.s, e)
}

func (s *stack[E]) Pop() (E, bool) {
	l := len(s.s)
	if l == 0 {
		return *new(E), false
	}

	val := s.s[l-1]
	s.s = s.s[:l-1]

	return val, true
}

func (s *stack[E]) Peek() (E, bool) {
	l := len(s.s)
	if l == 0 {
		return *new(E), false
	}

	return s.s[l-1], true
}

func (s *stack[E]) AllPop() iter.Seq[E] {
	return func(yield func(E) bool) {
		for {
			val, ok := s.Pop()
			if !ok {
				return
			}
			if !yield(val) {
				return
			}
		}
	}
}

func (s *stack[E]) AllPeek() iter.Seq[E] {
	return func(yield func(E) bool) {
		for i := len(s.s); i > 0; i-- {
			if !yield(s.s[i-1]) {
				return
			}
		}
	}
}
