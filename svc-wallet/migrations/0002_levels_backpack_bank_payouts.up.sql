-- Sender levels (lifetime coins spent on gifts), on the sender's shard.
CREATE TABLE IF NOT EXISTS user_stats (
  user_id      uuid PRIMARY KEY,
  coins_spent  bigint NOT NULL DEFAULT 0 CHECK (coins_spent >= 0),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- "Diamonds earned this week" on the receiver's shard.
CREATE INDEX IF NOT EXISTS gift_credits_receiver ON gift_credits (receiver_id, credited_at DESC);

-- Backpack: free gifts from daily check-ins. Never bought, never paid out.
CREATE TABLE IF NOT EXISTS backpack (
  user_id     uuid NOT NULL,
  gift_id     text NOT NULL,
  qty         int NOT NULL CHECK (qty >= 0),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, gift_id)
);
CREATE TABLE IF NOT EXISTS checkins (
  user_id  uuid NOT NULL,
  day      date NOT NULL,
  streak   int NOT NULL CHECK (streak > 0),
  PRIMARY KEY (user_id, day)
);
-- Idempotency for backpack sends (coin sends use transactions.idempotency_key).
CREATE TABLE IF NOT EXISTS backpack_sends (
  idempotency_key  text PRIMARY KEY,
  gift_send_id     uuid NOT NULL
);
ALTER TABLE gift_sends ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'coins';
ALTER TABLE gift_sends DROP CONSTRAINT IF EXISTS gift_sends_source_check;
ALTER TABLE gift_sends ADD CONSTRAINT gift_sends_source_check CHECK (source IN ('coins', 'backpack'));

-- Bank account for cash-outs (Paystack transfer recipient). The full account
-- number lives only at Paystack; we keep the last 4 digits for display.
CREATE TABLE IF NOT EXISTS payout_accounts (
  user_id         uuid PRIMARY KEY,
  bank_code       text NOT NULL,
  bank_name       text NOT NULL,
  account_last4   text NOT NULL,
  account_name    text NOT NULL,
  recipient_code  text NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE payouts DROP CONSTRAINT IF EXISTS payouts_status_check;
ALTER TABLE payouts ADD CONSTRAINT payouts_status_check
  CHECK (status IN ('pending_review', 'processing', 'paid', 'rejected', 'failed'));
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS amount_kobo bigint NOT NULL DEFAULT 0;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS recipient_code text;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS bank_label text NOT NULL DEFAULT '';
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS transfer_reference text;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS reviewed_by uuid;
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS note text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS payouts_transfer_ref ON payouts (transfer_reference) WHERE transfer_reference IS NOT NULL;
