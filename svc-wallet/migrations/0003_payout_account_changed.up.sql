-- When the cash-out bank account last changed to a different account
-- (NULL until the first change). Cash-outs pause for 48 hours after it.
ALTER TABLE payout_accounts ADD COLUMN IF NOT EXISTS changed_at timestamptz;
