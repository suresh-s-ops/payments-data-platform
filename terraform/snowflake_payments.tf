# Snowflake objects are managed as code: same setup in dev, QA and prod,
# and every access change is reviewed in a pull request (audit trail).

resource "snowflake_database" "payments" {
  name = "PAYMENTS_${upper(var.env)}"
}

resource "snowflake_warehouse" "transform" {
  name           = "TRANSFORM_WH_${upper(var.env)}"
  warehouse_size = "MEDIUM"
  auto_suspend   = 60          # stop paying for idle compute after 1 minute
  auto_resume    = true
}

resource "snowflake_account_role" "pii_reader" {
  name    = "PAYMENTS_PII_READER"
  comment = "Approved roles only: can see unmasked customer identifiers"
}

resource "snowflake_grant_privileges_to_account_role" "analyst_read" {
  account_role_name = "PAYMENTS_ANALYST"
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.payments.name
  }
}
