// Package feed reads the satellite fire feed over
// Server-Sent Events and resumes from a durable offset after restarts.
package feed

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// MaxLineBytes caps one SSE line. A longer line is discarded, and its event is
// marked Oversized so it can be dead-lettered instead of stalling the stream.
const MaxLineBytes = 1 << 20

// Event is one complete SSE event: the fields between two blank lines.
type Event struct {
	ID        string
	Event     string
	Data      string
	Oversized bool // a line of this event was longer than MaxLineBytes and was dropped
}

// ReadEvents parses an SSE stream and calls fn once for each complete event.
// Comment lines (starting with ':') are skipped. An event that is cut off
// before its blank line is discarded, as the SSE spec requires, so a torn
// write never reaches fn.
func ReadEvents(r io.Reader, fn func(Event) error) error {
	br := bufio.NewReaderSize(r, 64*1024)

	var cur Event
	var data []string
	open := false

	dispatch := func() error {
		if !open {
			return nil
		}
		cur.Data = strings.Join(data, "\n")
		ev := cur
		cur, data, open = Event{}, nil, false
		return fn(ev)
	}

	for {
		line, tooLong, err := readLine(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if tooLong {
			cur.Oversized = true
			open = true
			continue
		}
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			cur.ID = value
			open = true
		case "event":
			cur.Event = value
			open = true
		case "data":
			data = append(data, value)
			open = true
		}
	}
}

// readLine returns one line without its line ending. A line longer than
// MaxLineBytes is read to its end and dropped, and tooLong is set.
func readLine(br *bufio.Reader) (line string, tooLong bool, err error) {
	var b strings.Builder
	for {
		chunk, isPrefix, err := br.ReadLine()
		if err != nil {
			return "", false, err
		}
		if !tooLong {
			if b.Len()+len(chunk) > MaxLineBytes {
				tooLong = true
				b.Reset()
			} else {
				b.Write(chunk)
			}
		}
		if !isPrefix {
			if tooLong {
				return "", true, nil
			}
			return b.String(), false, nil
		}
	}
}
