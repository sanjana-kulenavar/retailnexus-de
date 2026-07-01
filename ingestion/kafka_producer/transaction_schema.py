# ingestion/kafka_producer/transaction_schema.py

# Pydantic is a data validation library.
# You define the "shape" of your data as a Python class, and Pydantic
# automatically checks that every value matches the rules you set.
# If a field is missing or has the wrong type/range, Pydantic raises an error.
from pydantic import BaseModel, Field, field_validator
from typing import Literal


class POSTransaction(BaseModel):
    """
    Represents a single Point-of-Sale transaction from a Dutch retail store.

    This is the modern equivalent of an Oracle ReSA RTLOG transaction record,
    expressed as a validated Python object instead of a fixed-width file line.

    Every field below has validation rules. If the producer ever tries to
    create a transaction that breaks these rules, Pydantic stops it
    immediately — so malformed data never reaches Kafka.
    """

    # A globally unique ID for this transaction.
    # In ReSA terms, think of this like the RTLOG sequence/transaction number.
    transaction_id: str

    # Which store the sale happened in. Format: "NL-001" through "NL-050".
    store_id: str

    # Which checkout terminal inside the store. Format: "POS-01", "POS-02", etc.
    terminal_id: str

    # The employee who processed the sale. Format: "EMP-1234".
    cashier_id: str

    # The product that was scanned. Format: "PROD-00001".
    product_id: str

    # How many units were bought.
    # Field(ge=1, le=100) means: must be >= 1 and <= 100.
    # ge = "greater than or equal", le = "less than or equal".
    quantity: int = Field(ge=1, le=100)

    # Price per single unit, in euros.
    # Must be between 0.01 and 9999.99 — no free or absurdly priced items.
    unit_price: float = Field(ge=0.01, le=9999.99)

    # Total value of the line = quantity * unit_price.
    # We validate this below to make sure it was calculated correctly.
    total_amount: float

    # How the customer paid.
    # Literal[...] means the value MUST be exactly one of these four strings.
    # Anything else (e.g. "BITCOIN") would be rejected.
    payment_method: Literal["CARD", "CASH", "CONTACTLESS", "VOUCHER"]

    # When the transaction happened, as an ISO-format timestamp string.
    # Example: "2026-06-19T14:30:00.123456"
    transaction_ts: str

    # Was this transaction cancelled/reversed?
    # Defaults to False. About 2% of real retail transactions are voided.
    is_voided: bool = False

    # The audit classification of this transaction.
    # This mirrors the ReSA audit flag concept — every transaction is
    # categorised so the Sales Audit process knows how to treat it.
    audit_flag: Literal["VALID", "VOIDED", "EXCEPTION_HIGH_VALUE"] = "VALID"

    # ── Custom validation ────────────────────────────────────────────
    # A @field_validator runs automatically whenever a POSTransaction is
    # created. This one checks that total_amount equals quantity * unit_price.
    # In Oracle ReSA, a mismatched total was a classic audit exception —
    # we catch the same problem here, at the source.
    @field_validator("total_amount")
    @classmethod
    def validate_total_amount(cls, total_amount, info):
        """
        Verify total_amount == quantity * unit_price (within 1 cent for rounding).

        `info.data` holds the other fields that have already been validated.
        We only check if both quantity and unit_price are present.
        """
        if "quantity" in info.data and "unit_price" in info.data:
            expected = round(info.data["quantity"] * info.data["unit_price"], 2)
            # Allow a 1-cent tolerance for floating-point rounding
            if abs(total_amount - expected) > 0.01:
                raise ValueError(
                    f"total_amount {total_amount} does not match "
                    f"quantity * unit_price = {expected}"
                )
        return total_amount
