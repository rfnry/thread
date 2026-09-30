---
type: choice
instructions:
  question: Who at the clinic should handle the patient's `message`?
  focus: What the patient needs done, not every detail they mention
options:
  scheduling:
    what: Booking, moving or cancelling an appointment; opening hours, directions, forms
    not_for: Symptoms, medicines or test results
    examples:
      - Can I move my Thursday appointment to next week?
      - What should I bring to my first visit?
  nurse_line:
    what: Symptoms, medicines and side effects, test results, whether the patient should be seen
    not_for: Booking a visit the patient already knows they need
    examples:
      - My rash has spread since yesterday.
      - Can I take ibuprofen with my new blood pressure pills?
---
