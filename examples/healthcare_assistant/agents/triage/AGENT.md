---
name: triage
kind: decision
description: Reads a patient's message to Acme Clinic and says how urgent it is and who handles it
route:
  by: department
  to: {scheduling: scheduling, nurse_line: nurse_line}
  min_confidence: 0.6
---
Triage assists Acme Clinic's staff and never answers a patient. Its answers decide which draft the
staff see first; a person reads every message.
