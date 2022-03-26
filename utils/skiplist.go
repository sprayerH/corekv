package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
		},
		rand:     r,
		maxLevel: defaultMaxLevel - 1,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	prevs := make([]*Element, list.maxLevel+1)

	key := data.Key
	score := list.calcScore(data.Key)
	prev := list.header
	maxLevel := list.maxLevel
	for i := maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if result := list.compare(score, key, next); result <= 0 {
				if result == 0 {
					next.entry = data
					return nil
				} else {
					prev = next
				}
			} else {
				break
			}
		}
		prevs[i] = prev
	}

	randomLevel := list.randLevel()
	elem := newElement(score, data, randomLevel+1)
	for i := 0; i <= randomLevel; i++ {
		next := prevs[i].levels[i]
		prevs[i].levels[i] = elem
		elem.levels[i] = next
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	score := list.calcScore(key)
	prev := list.header
	for i := list.maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if result := list.compare(score, key, next); result <= 0 {
				if result == 0 {
					return next.entry
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	for i := 1; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
