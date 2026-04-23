// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"

	"go.uber.org/zap"
)

type diffEntry uint

type diffEntryWithFlags diffEntry

const (
	diffEntryMask           diffEntryWithFlags = 0xf
	diffEntryInheritRemoved diffEntryWithFlags = 0x10

	undefPred = ^uint(0)

	baseReaderSource = -1
)

func diffEntrySetFlags(e diffEntry, f diffEntryWithFlags) diffEntryWithFlags {
	return diffEntryWithFlags(e) | f
}

func (e diffEntryWithFlags) diffEntry() diffEntry {
	return diffEntry(e & diffEntryMask)
}

type readEntryF func(p int, r *bufio.Reader, f diffEntryWithFlags) error

func (c deltaSpanT) copy() deltaSpanT {
	n := deltaSpanT{}
	n.deltas = append(make([]int, 0, len(c.deltas)), c.deltas...)

	count := *c.cumulativeCount
	n.cumulativeCount = &count

	itc := *c.identicalCount
	n.identicalCount = &itc

	n.entries = c.entries
	return n
}

// countDeltas calculates the delta values for a base reader and a series of diff readers.
//
// The function initializes the `deltas`, `counts`, and `span` slices based on the number of diff readers.
// It then calls `readCountDelta` to populate the `deltas` slice with computed delta values.
// Finally, it calculates the cumulative sum of the deltas using `_countInitDeltas`.
//
// Parameters:
//   - rBase: A buffered reader for the base data. Can be nil if no base data is provided.
//   - rDiffs: A slice of buffered readers for the diff data.
//
// Returns:
//   - count: The total sum of all elements in the `deltas` slice.
//   - err: An error if any of the readers encounters an issue while reading data, or nil if the operation completes successfully.
//
// Note:
//   - The `deltas` slice will have a length equal to len(rDiffs) + 1.
//   - The function assumes that the `readCountDelta` function is available and correctly processes the readers.
func (c *deltaSpanT) countDeltas(rBase *bufio.Reader, rDiffs *bufio.Reader) (count int, err error) {
	c.deltas = make([]int, 2)
	c.cumulativeCount = new(int)
	c.identicalCount = new(uint)
	err = readCountDelta(c.deltas, rBase, rDiffs)
	if err != nil {
		return
	}
	count = c._countInitDeltas()
	return
}

// readCountDelta reads delta values from a base reader and a series of diff readers,
// and populates the provided deltas slice with the computed values.
//
// The function processes the diff readers recursively, starting from the last one
// and working backwards. For each diff reader, it reads two unsigned integers
// (representing removed and added counts), calculates the delta as the difference
// between the added and removed counts, and stores it in the corresponding position
// in the deltas slice. Once all diff readers are processed, it reads a base count
// from the base reader (if provided) and stores it in the first position of the deltas slice.
//
// Parameters:
//   - deltas: A slice of integers where the computed delta values will be stored.
//   - rBase: A buffered reader for the base data. Can be nil if no base data is provided.
//   - rDiffs: A slice of buffered readers for the diff data. Each reader corresponds to
//     a level of delta computation.
//
// Returns:
//   - An error if any of the readers encounters an issue while reading data, or nil if
//     the operation completes successfully.
//
// Note:
//   - The deltas slice must have a length equal to len(rDiffs) + 1.
//   - The function assumes that the readUint function is available and correctly reads
//     unsigned integers from the provided readers.
func readCountDelta(deltas []int, rBase *bufio.Reader, rDiffs *bufio.Reader) error {
	if rDiffs != nil {
		var d int
		// read diff
		removed, err := readUint(rDiffs)
		if err != nil {
			return err
		}
		GlobalLogger.Debug("readUint from diff", zap.Uint("removed", removed))

		added, err := readUint(rDiffs)
		if err != nil {
			return err
		}
		GlobalLogger.Debug("readUint from diff", zap.Uint("added", added))
		d = int(added) - int(removed)

		deltas[1] = d

		return readCountDelta(deltas[:1], rBase, nil)
	}
	var c uint
	if rBase != nil {
		var err error
		c, err = readUint(rBase)
		GlobalLogger.Debug("readUint from base", zap.Uint("base", c))
		if err != nil {
			return err
		}
	}
	deltas[0] = int(c)
	return nil
}

// _countInitDeltas calculates the cumulative sum of the elements in the `deltas` slice
// and updates the `counts` slice with the cumulative sums (excluding the last element).
// It returns the total sum of all elements in the `deltas` slice.
//
// The function iterates over the `deltas` slice, adding each element to a running total (`count`).
// For every element except the first, it assigns the current cumulative sum to the corresponding
// index in the `counts` slice (offset by -1).
//
// Returns:
//
//	int: The total sum of all elements in the `deltas` slice.
func (c *deltaSpanT) _countInitDeltas() int {
	count := int(c.deltas[0]) + int(c.deltas[1])
	c.cumulativeCount = &count
	return count
}

func (c *deltaNodeSpanT) _countInitDeltas() uint {
	count := (*c.baseTripleCount) + (*c.addedTripleCount) - (*c.removedTripleCount)
	c.cumulativeCount = &count
	return count
}

// read entries from base and diff file into deltaSpan
func (deltaSpan *deltaSpanT) readEntries(rBase *bufio.Reader, rDiff *bufio.Reader, f readEntryF) error {
	var nextEntry uint
	for {
		done, err := deltaSpan.readNext(rBase, rDiff, &nextEntry, f, 0)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// a helper method for readEntries
func (deltaSpan *deltaSpanT) readNext(baseReader *bufio.Reader, diffReader *bufio.Reader, nextEntries *uint, entryCallback readEntryF, inheritedFlags diffEntryWithFlags) (bool, error) {
	if diffReader != nil {
		for {
			var isTripleModified bool
		loop:
			for *deltaSpan.identicalCount == 0 {
				if *deltaSpan.cumulativeCount == 0 && deltaSpan.deltas[1] == 0 {
					return deltaSpan.readNext(baseReader, nil, nextEntries, entryCallback, inheritedFlags)
				}
				// nextEntry := nextEntries[0]
				if *nextEntries != 0 { // if nextEntry is set
					*nextEntries--
				} else { // if nextEntry is not set, read from diff
					var err error
					*nextEntries, err = readUint(diffReader)
					switch diffEntry(*nextEntries) {
					case diffEntrySame:
						GlobalLogger.Debug("readUint from diff - diffEntryFlag diffEntrySame 0")
					case diffEntryRemoved:
						GlobalLogger.Debug("readUint from diff - diffEntryFlag diffEntryRemoved 1")
					case diffEntryAdded:
						GlobalLogger.Debug("readUint from diff - diffEntryFlag diffEntryAdded 2")
					case diffEntryTripleModified:
						GlobalLogger.Debug("readUint from diff - diffEntryFlag diffEntryTripleModified 3")
					default:
						GlobalLogger.Panic("readUint from diff - diffEntryFlag unknown")
					}
					if err != nil {
						return false, err
					}
				}
				switch diffEntry(*nextEntries) {
				case diffEntrySame:
					var err error
					// read same count
					*deltaSpan.identicalCount, err = readUint(diffReader)
					GlobalLogger.Debug("readUint from diff", zap.Uint("same count", *deltaSpan.identicalCount))
					if err != nil {
						return false, err
					}
					if *deltaSpan.identicalCount == 0 {
						continue
					}
					if deltaSpan.entries != nil {
						deltaSpan.entries = append(deltaSpan.entries, diffEntrySpan{
							diffEntry: diffEntrySame,
							span:      *deltaSpan.identicalCount,
						})
					}
				case diffEntryRemoved:
					err := entryCallback(0, diffReader, diffEntrySetFlags(diffEntryRemoved, diffEntryInheritRemoved))
					if err != nil {
						return false, err
					}
					done, err := deltaSpan.readNext(baseReader, nil, nextEntries, entryCallback, diffEntryInheritRemoved)
					deltaSpan.deltas[1]++
					if err != nil {
						return false, err
					}
					if done {
						panic(ErrMisalignedDiff)
					}
					if deltaSpan.entries != nil {
						deltaSpan.entries = append(deltaSpan.entries, diffEntrySpan{
							diffEntry: diffEntryRemoved,
							span:      1,
						})
					}
					break loop
				case diffEntryAdded:
					err := entryCallback(0, diffReader, diffEntrySetFlags(diffEntryAdded, inheritedFlags))
					deltaSpan.deltas[1]--
					*deltaSpan.cumulativeCount--
					if err != nil {
						return false, err
					}
					if deltaSpan.entries != nil {
						deltaSpan.entries = append(deltaSpan.entries, diffEntrySpan{
							diffEntry: diffEntryAdded,
							span:      1,
						})
					}
					break loop
				case diffEntryTripleModified:
					err := entryCallback(0, diffReader, diffEntrySetFlags(diffEntryTripleModified, inheritedFlags))
					if err != nil {
						return false, err
					}
					*deltaSpan.identicalCount = 1
					if deltaSpan.entries != nil {
						deltaSpan.entries = append(deltaSpan.entries, diffEntrySpan{
							diffEntry: diffEntryTripleModified,
							span:      1,
						})
					}
					isTripleModified = true
				default:
					GlobalLogger.Panic(ErrUnrecognizedDiffEntry.Error())
				}
			}
			if *deltaSpan.identicalCount != 0 { // when span is not equal to 0, it means there are content needs to be read from base
				*deltaSpan.identicalCount--
				*deltaSpan.cumulativeCount--
				if !isTripleModified {
					err := entryCallback(0, diffReader, diffEntrySetFlags(diffEntrySame, inheritedFlags))
					if err != nil {
						return false, err
					}
					isTripleModified = false
				}
				_, err := deltaSpan.readNext(baseReader, nil, nextEntries, entryCallback, inheritedFlags)
				if err != nil {
					return false, err
				}
			}
			if *deltaSpan.identicalCount == 0 && (*deltaSpan.cumulativeCount != 0 || deltaSpan.deltas[1] != 0) { // means there are content in the diff
				nextEntry, err := readUint(diffReader)
				GlobalLogger.Debug("readUint from diff", zap.Uint("nextEntry", nextEntry))
				if err != nil {
					return false, err
				}
				*nextEntries = nextEntry + 1
				if diffEntry(nextEntry) == diffEntryRemoved {
					continue
				}
			} else {
				*nextEntries = 0
			}
			break
		}
		return false, nil
	} else {
		// check if baseNG still has left things to handle
		if deltaSpan.deltas[0] == 0 {
			return true, nil
		}
		err := entryCallback(-1, baseReader, diffEntrySetFlags(diffEntryAdded, inheritedFlags))
		if err != nil {
			return false, err
		}
		// handled one for the baseNG, so decrement the deltas[0]
		deltaSpan.deltas[0]--
		return false, err
	}
}

// iterate all entries of deltaSpan
func (deltaSpan deltaSpanT) forEntries(rBase *bufio.Reader, rDiffs *bufio.Reader, callback readEntryF) error {
	cc := deltaSpan.copy() // Create a copy of the deltaSpanT instance
	var p int              // Track positions in the entries
	for {
		// Process the next entry using the helper method forNext
		done, err := cc.forNext(rBase, rDiffs, &p, callback, 0)
		if err != nil {
			return err
		}
		if done {
			return nil // All entries processed
		}
	}
}

// a helper method for forEntries
func (deltaSpan deltaSpanT) forNext(rBase *bufio.Reader, rDiff *bufio.Reader, entryIndex *int, callback readEntryF, ie diffEntryWithFlags) (bool, error) {
	// if there are no diff readers, process the base reader
	if rDiff != nil {
		for {
			// flag to track if the current entry is tripleModified
			var tripleModified bool
		loop:
			// check if the current diff reader has entries to process
			// if currentDiffReaderEntryCount == 0, it means the current diff reader is exhausted
			// and we need to check the next diff reader
			for *deltaSpan.identicalCount == 0 {
				// if there are no entries left in the current diff reader and the delta count is 0,
				// we can process the next diff reader
				if *deltaSpan.cumulativeCount == 0 && deltaSpan.deltas[1] == 0 {
					return deltaSpan.forNext(rBase, nil, entryIndex, callback, ie)
				}
				// read an entry from the current diff reader
				entry := deltaSpan.entries[*entryIndex]
				*entryIndex++
				switch entry.diffEntry {
				case diffEntrySame:
					*deltaSpan.identicalCount = entry.span
					if *deltaSpan.identicalCount == 0 {
						continue
					}
				case diffEntryRemoved:
					err := callback(0, rDiff, diffEntrySetFlags(diffEntryRemoved, diffEntryInheritRemoved))
					if err != nil {
						return false, err
					}
					done, err := deltaSpan.forNext(rBase, nil, entryIndex, callback, diffEntryInheritRemoved)
					deltaSpan.deltas[1]++
					if err != nil {
						return false, err
					}
					if done {
						// return done, nil
						panic(ErrMisalignedDiff)
						// return false, ErrMisalignedDiff
					}
					break loop
				case diffEntryAdded:
					err := callback(0, rDiff, diffEntrySetFlags(diffEntryAdded, ie))
					deltaSpan.deltas[1]--
					*deltaSpan.cumulativeCount--
					if err != nil {
						return false, err
					}
					break loop
				case diffEntryTripleModified:
					err := callback(0, rDiff, diffEntrySetFlags(diffEntryTripleModified, ie))
					if err != nil {
						return false, err
					}
					*deltaSpan.identicalCount = 1
					tripleModified = true
				default:
					GlobalLogger.Panic(ErrUnrecognizedDiffEntry.Error())
				}
			}
			if *deltaSpan.identicalCount != 0 { // when span is not equal to 0, it means there are content needs to be read from base
				*deltaSpan.identicalCount--
				*deltaSpan.cumulativeCount--
				if !tripleModified {
					err := callback(0, rDiff, diffEntrySetFlags(diffEntrySame, ie))
					if err != nil {
						return false, err
					}
					tripleModified = false
				}
				_, err := deltaSpan.forNext(rBase, nil, entryIndex, callback, ie)
				if err != nil {
					return false, err
				}
			}
			if *deltaSpan.identicalCount == 0 && (*deltaSpan.cumulativeCount != 0 || deltaSpan.deltas[1] != 0) {
				e := deltaSpan.entries[*entryIndex]
				if e.diffEntry == diffEntryRemoved {
					continue
				}
			}
			break
		}
		return false, nil
	} else {
		if deltaSpan.deltas[0] == 0 {
			return true, nil
		}
		err := callback(baseReaderSource, rBase, diffEntrySetFlags(diffEntryAdded, ie))
		if err != nil {
			return false, err
		}
		deltaSpan.deltas[0]--
		return false, err
	}
}

// p indicating the r source
// -1 is the base file
// 0 is the diff file
type readNodeEntryF func(p int, r *bufio.Reader, f diffEntryWithFlags, pred uint) error

type deltaNodeSpanT struct {
	// baseTripleCount is the count of triples in the base reader
	baseTripleCount *uint

	// addedTripleCount is the count of triples added in the diff reader
	addedTripleCount *uint

	// removedTripleCount is the count of triples removed in the diff reader
	removedTripleCount *uint

	// baseTripleCount + addedTripleCount - removedTripleCount
	// cumulativeCount is the cumulative count of triples after applying the deltas
	cumulativeCount *uint

	// Number of identical triples in the diff reader
	identicalTripleCount *uint
}

func (c *deltaNodeSpanT) countDeltas(
	rBase *bufio.Reader, rDiffs *bufio.Reader,
	identicalNodeCount *uint, beforeReaderSourceIndex int,
) (count int, err error) {
	c.cumulativeCount = new(uint)
	c.identicalTripleCount = new(uint)
	c.baseTripleCount = new(uint)
	c.addedTripleCount = new(uint)
	c.removedTripleCount = new(uint)
	var cBase *bufio.Reader
	if beforeReaderSourceIndex < 0 {
		cBase = rBase
	}
	err = readCountDeltaNodeNodes(c, cBase, rDiffs, identicalNodeCount)
	if err != nil {
		return
	}
	count = int(c._countInitDeltas())
	return
}

func readCountDeltaNodeNodes(
	c *deltaNodeSpanT, rBase *bufio.Reader, rDiff *bufio.Reader,
	identicalNodeCount *uint,
) error {
	if rDiff != nil {
		for {
			if *identicalNodeCount > 1 {
				*identicalNodeCount--
				GlobalLogger.Debug("identicalNodeCount--", zap.Uint("identicalNodeCount", *identicalNodeCount))
				break
			} else {
				*identicalNodeCount = 0
				removed, err := readUint(rDiff)
				if err != nil {
					return err
				}
				if removed&diffModifiedFlag == 0 {
					*identicalNodeCount = (removed >> diffIdenticalOrModifiedOffset) + 1 // problem?? why added one
					GlobalLogger.Debug("readUint from diff", zap.Uint("identical span", *identicalNodeCount-1))
					continue
				}

				removed = removed >> diffIdenticalOrModifiedOffset
				GlobalLogger.Debug("readUint from diff", zap.Uint("removed non-literal triple count", removed))
				added, err := readUint(rDiff)
				GlobalLogger.Debug("readUint from diff", zap.Uint("added non-literal triple count", added))
				if err != nil {
					return err
				}
				c.addedTripleCount = &added
				c.removedTripleCount = &removed
				break
			}
		}
		return readCountDeltaNodeNodes(c, rBase, nil, identicalNodeCount)
	} else {
		var ntc uint
		if rBase != nil {
			var err error
			ntc, err = readUint(rBase)
			GlobalLogger.Debug("readUint from base", zap.Uint("non-literal triple count", ntc))
			if err != nil {
				return err
			}
		}
		c.baseTripleCount = &ntc
		return nil
	}
}

func (c *deltaNodeSpanT) countLiteralDeltas(
	rBase *bufio.Reader, rDiff *bufio.Reader,
	identicalNodeCount *uint, readerSource int,
) (count int, err error) {
	c.baseTripleCount = new(uint)
	c.addedTripleCount = new(uint)
	c.removedTripleCount = new(uint)
	c.cumulativeCount = new(uint)
	c.identicalTripleCount = new(uint)
	var cBase *bufio.Reader
	if readerSource < 0 {
		cBase = rBase
	}
	err = readCountDeltaNodeLiterals(c, cBase, rDiff, identicalNodeCount)
	if err != nil {
		return
	}
	count = int(c._countInitDeltas())
	return
}

func readCountDeltaNodeLiterals(
	c *deltaNodeSpanT, rBase *bufio.Reader, rDiff *bufio.Reader,
	nodeSpans *uint,
) error {
	if rDiff != nil {
		if *nodeSpans > 0 {
			// do nothing
		} else {
			r := rDiff
			removed, err := readUint(r)
			GlobalLogger.Debug("readUint from diff", zap.Uint("removed literal triple count", removed))
			if err != nil {
				return err
			}
			if removed&diffModifiedFlag != diffModifiedFlag {
				GlobalLogger.Panic(ErrUnrecognizedDiffEntry.Error())
			}
			removed >>= diffIdenticalOrModifiedOffset
			added, err := readUint(r)
			GlobalLogger.Debug("readUint from diff", zap.Uint("added literal triple count", added))
			if err != nil {
				return err
			}
			c.addedTripleCount = &added
			c.removedTripleCount = &removed
		}
		return readCountDeltaNodeLiterals(c, rBase, nil, nodeSpans)
	} else {
		var utc uint
		if rBase != nil {
			var err error
			utc, err = readUint(rBase)
			GlobalLogger.Debug("readUint from base", zap.Uint("base literal count", utc))
			if err != nil {
				return err
			}
		}
		c.baseTripleCount = &utc
		return nil
	}
}

// read each deltaNodeSpanT entries
func (c deltaNodeSpanT) readEntries(
	rBase *bufio.Reader, rDiff *bufio.Reader,
	identicalNodeCount *uint, readerSource int, f readNodeEntryF,
) error {
	var cBase *bufio.Reader
	if readerSource < 0 {
		cBase = rBase
	}
	var nextEntry uint = 0
	for {
		done, err := c.readNext(cBase, rDiff, identicalNodeCount, &nextEntry, f, 0)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// a helper method for (c deltaNodeSpanT) readEntries
func (c deltaNodeSpanT) readNext(rBase *bufio.Reader, rDiff *bufio.Reader, identicalNodeCount *uint, nextEntry *uint, callback readNodeEntryF, flag diffEntryWithFlags) (bool, error) {
	// GlobalLogger.Debug("readNext", zap.Uint("identicalNodeCount", *identicalNodeCount), zap.Uint("nextEntry", *nextEntry), zap.Uint("flag", uint(flag)))
	if rDiff != nil {
		if *identicalNodeCount > 0 {
			// for setting translation index
			err := callback(0, nil, diffEntrySetFlags(diffEntrySame, flag), undefPred)
			if err != nil {
				return false, err
			}
			return c.readNext(rBase, nil, identicalNodeCount, nextEntry, callback, flag)
		}
		for {
		loop:
			for *c.identicalTripleCount == 0 {
				// when there is nothing to read in the diff
				if *c.addedTripleCount == 0 && *c.removedTripleCount == 0 {
					// means there is nothing to be read in the base
					if *c.cumulativeCount == 0 {
						return true, nil
					}
					// also means there is nothing to be read in the base
					if *c.baseTripleCount == 0 {
						return true, nil
					} else {
						// means there are content in base needs to be read
						// added for modified triple of literal
						err := callback(0, nil, diffEntrySetFlags(diffEntrySame, flag), undefPred)
						if err != nil {
							return false, err
						}
						return c.readNext(rBase, nil, identicalNodeCount, nextEntry, callback, flag)
					}
				}

				if *nextEntry != 0 {
					*nextEntry--
				} else {
					var err error
					// read predicate index with flags
					*nextEntry, err = readUint(rDiff)
					GlobalLogger.Debug("readUint from diff", zap.Uint("predicate Index", *nextEntry>>diffSameRemovedOrAddedOffset), zap.Uint("shifted", *nextEntry))
					if err != nil {
						return false, err
					}
				}
				predIndex := *nextEntry >> diffSameRemovedOrAddedOffset // v is e shifted by 2 bits to get the value of the most significant 6 bits
				switch *nextEntry & diffSameRemovedOrAddedMask {        // look for the least significant 2 bits to decode the tag
				case 0: // both bits are 0b00
					// in this case, predIndex means the identical triple count
					*c.identicalTripleCount = predIndex
					if *c.identicalTripleCount == 0 {
						continue
					}
				case diffRemovedFlag: // bits are 0b01
					err := callback(0, rDiff, diffEntrySetFlags(diffEntryRemoved, diffEntryInheritRemoved), predIndex)
					if err != nil {
						return false, err
					}
					*c.removedTripleCount--
					done, err := c.readNext(rBase, nil, identicalNodeCount, nextEntry, callback, diffEntryInheritRemoved)
					if err != nil {
						return false, err
					}
					if done {
						return done, nil
						// panic(ErrMisalignedDiff)
						// return false, ErrMisalignedDiff
					}
					break loop
				case diffAddedFlag: // bits are 0b10
					err := callback(0, rDiff, diffEntrySetFlags(diffEntryAdded, flag), predIndex)
					*c.addedTripleCount--
					*c.cumulativeCount--
					if err != nil {
						return false, err
					}
					break loop
				default: // bits are 0b11
					GlobalLogger.Panic(ErrUnrecognizedDiffEntry.Error())
				}

			}
			if *c.identicalTripleCount != 0 {
				*c.identicalTripleCount--
				*c.cumulativeCount--
				err := callback(0, rDiff, diffEntrySetFlags(diffEntrySame, flag), undefPred)
				if err != nil {
					return false, err
				}
				_, err = c.readNext(rBase, nil, identicalNodeCount, nextEntry, callback, flag)
				if err != nil {
					return false, err
				}
			}
			if *c.identicalTripleCount == 0 && (*c.cumulativeCount != 0 || *c.removedTripleCount != 0 || *c.addedTripleCount != 0) {
				entryFlag, err := readUint(rDiff)
				GlobalLogger.Debug("readUint from diff", zap.Uint("entryFlag", entryFlag))
				if err != nil {
					return false, err
				}
				*nextEntry = entryFlag + 1
				if diffEntry(entryFlag) == diffEntryRemoved {
					continue
				}
			} else {
				*nextEntry = 0
			}
			break
		}
		return false, nil
	} else {
		if *c.baseTripleCount == 0 {
			return true, nil
		}
		pred, err := readUint(rBase)
		if err != nil {
			GlobalLogger.Error(err.Error())
		}
		GlobalLogger.Debug("readUint from base", zap.Uint("predicate index", pred))
		err = callback(-1, rBase, diffEntrySetFlags(diffEntryAdded, flag), pred)
		if err != nil {
			GlobalLogger.Error(err.Error())
		}
		*c.baseTripleCount--
		return false, err
	}
}
