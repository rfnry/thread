from rfnry_thread import Text

COMPLAINT = (
    "New case from Joana Ribeiro, by email this morning: 'My Aurora espresso machine arrived on "
    "the 12th with a cracked water tank and it leaks all over the counter. I want my money back.' "
    "She attached her receipt and the invoice. What do we do?"
)
CORRECTION = (
    "Offer to ship her a new water tank at no cost first; only if she still wants her money back, "
    "escalate to a team lead, since the total is over $500."
)
DISAGREEMENT = f"I disagree. The tank is a spare part we stock. {CORRECTION}"
INVOICE = [
    "HARBOR HOME APPLIANCES - INVOICE INV-77120",
    "Order A-20931",
    "Ordered 2026-09-08    Delivered 2026-09-12",
    "Bill to Joana Ribeiro, Rua das Flores 18, Porto",
    "",
    "Aurora espresso machine     1 x 549.00",
    "Shipping                        0.00",
    "Tax 8%                         43.92",
    "Total USD                     592.92",
    "",
    "Paid by VISA **** 4417 on 2026-09-08",
]


def invoice_pdf(lines):
    stream = "BT /F1 11 Tf 56 780 Td 16 TL " + "".join(f"({line}) Tj T* " for line in lines) + "ET"
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
    ]
    pdf, offsets = b"%PDF-1.4\n", []
    for number, body in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf += f"{number} 0 obj\n{body}\nendobj\n".encode()
    table = "".join(f"{offset:010} 00000 n \n" for offset in offsets)
    size = len(objects) + 1
    return (
        pdf
        + (
            f"xref\n0 {size}\n0000000000 65535 f \n{table}"
            f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{len(pdf)}\n%%EOF\n"
        ).encode()
    )


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))
