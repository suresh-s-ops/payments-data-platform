from dataclasses import dataclass
from decimal import Decimal


@dataclass
class ReconResult:
    business_date: str
    source_rows: int
    target_rows: int
    source_amount: Decimal
    target_amount: Decimal

    @property
    def passed(self) -> bool:
        return (self.source_rows == self.target_rows
                and abs(self.source_amount - self.target_amount) <= Decimal("0.01"))


RECON_SQL = """
    SELECT COUNT(*), COALESCE(SUM(settled_amount), 0)
    FROM {table}
    WHERE {date_col} = %(d)s
"""


def reconcile(legacy_cur, snowflake_cur, business_date: str) -> ReconResult:
    """Compare legacy vs. new platform for one business date.

    Business rule: finance signs off on cutover only when row counts AND
    settled dollars match to the cent. A $0.02 gap on payments data is a
    real accounting break, not rounding noise.
    """
    legacy_cur.execute(RECON_SQL.format(table="payments.settlements",
                                        date_col="CAST(updated_at AS DATE)"),
                       {"d": business_date})
    src_rows, src_amt = legacy_cur.fetchone()

    snowflake_cur.execute(RECON_SQL.format(table="gold.payments.fct_payment_attempts",
                                           date_col="attempt_date"),
                          {"d": business_date})
    tgt_rows, tgt_amt = snowflake_cur.fetchone()

    return ReconResult(business_date, src_rows, tgt_rows,
                       Decimal(src_amt), Decimal(tgt_amt))
