// Package feed reads the satellite fire feed (Spesifikasi M2, P4) over
// Server-Sent Events and resumes from a durable offset after restarts.
package feed

import (
	"bufio"
	"io"
	"strings"
)

// Event is one complete SSE event: the fields between two blank lines.
type Event struct {
	ID    string
	Event string
	Data  string
}

// ReadEvents parses an SSE stream and calls fn once for each complete event.
// Comment lines (starting with ':') are skipped. An event that is cut off
// before its blank line is discarded, as the SSE spec requires, so a torn
// write never reaches fn.
func ReadEvents(r io.Reader, fn func(Event) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)

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

	for sc.Scan() {
		line := sc.Text()
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
	return sc.Err()
}
