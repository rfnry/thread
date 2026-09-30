ledger.csv has one row per transaction: date (YYYY-MM-DD), account, counterparty, reference, and
amount in US dollars, negative for money out and positive for money in.

1. Two rows with the same counterparty, reference and amount are a duplicate. Report both by their
   line number in the file, and delete neither: the accountant decides.
2. Expenses are the rows with a negative amount, totalled by account, as positive numbers.
3. Round to cents only when you print or write a result.
