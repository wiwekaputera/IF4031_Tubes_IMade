package event

import "time"

// JetStream names for the priority alert path. Critical coastal observations
// are published here directly by the sensor poller, separate from the regular
// HazardEvent flow (ingest -> canonical store -> outbox -> BNPB_EVENTS), so a
// backlog on the regular path never delays them.
//
// Messages are Envelope values. EventID is the canonical hazard_id of the
// alert's HazardEvent and Version is 1, so a receiver that also gets the same
// event from BNPB_EVENTS recognises it as already delivered.
const (
	AlertStreamName     = "BNPB_ALERTS"
	AlertSubjectFilter  = "bnpb.alerts.>"
	CoastalAlertSubject = "bnpb.alerts.coastal"

	// AlertMaxAge bounds how long undelivered alerts are kept.
	AlertMaxAge = 24 * time.Hour
	// AlertDuplicateWindow lets JetStream drop a re-published alert for as
	// long as alerts are kept, so a poller restart that reads a page again
	// never counts an alert twice.
	AlertDuplicateWindow = AlertMaxAge

	// AttrDetectedAt is the HazardEvent attribute holding the RFC 3339 time at
	// which BNPB first read the observation; delivery latency is measured from it.
	AttrDetectedAt = "detected_at"
)
