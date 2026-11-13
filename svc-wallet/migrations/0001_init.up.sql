-- Wallet schema. Applied to EVERY physical shard (identical schema).
-- Money rules:
--   * Every movement is a transaction with balanced entries (sum = 0).
--   * User balances are cached in accounts.balance and only change inside the
--     same DB transaction that writes the entries.
--   * Platform accounts never update their balance column (that row would be
--     a global lock); their balance is SUM(entries), checked nightly.

CREATE TABLE accounts (
  id          uuid PRIMARY KEY,
  owner_id    uuid NOT NULL,
  kind        text NOT NULL CHECK (kind IN ('coins', 'diamonds',
                'platform_coin_issuance', 'platform_gift_clearing', 'platform_diamond_issuance',
                'platform_payout_clearing', 'platform_refunds')),
  balance     bigint NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (owner_id, kind)
);

-- Platform accounts (owner = nil UUID), one set per shard.
INSERT INTO accounts (id, owner_id, kind) VALUES
  ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000000', 'platform_coin_issuance'),
  ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000000', 'platform_gift_clearing'),
  ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000000', 'platform_diamond_issuance'),
  ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000000', 'platform_payout_clearing'),
  ('00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000000', 'platform_refunds');

CREATE TABLE transactions (
  id               uuid PRIMARY KEY,
  kind             text NOT NULL,  -- purchase | gift_debit | gift_credit | payout | reversal | adjustment
  idempotency_key  text NOT NULL,
  metadata         jsonb NOT NULL DEFAULT '{}',
  created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX transactions_idem ON transactions (idempotency_key);

-- Entries are append-only and partitioned by month so old months can be
-- archived (detach + export) without touching hot data.
CREATE TABLE entries (
  id          bigserial,
  tx_id       uuid NOT NULL,
  account_id  uuid NOT NULL,
  amount      bigint NOT NULL CHECK (amount <> 0),
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX entries_account ON entries (account_id, id DESC);
CREATE INDEX entries_tx ON entries (tx_id);
CREATE TABLE entries_default PARTITION OF entries DEFAULT;

CREATE OR REPLACE FUNCTION wallet_ensure_partitions(months_ahead int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  m date;
BEGIN
  FOR i IN 0..months_ahead LOOP
    m := (date_trunc('month', now()) + make_interval(months => i))::date;
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF entries FOR VALUES FROM (%L) TO (%L)',
      'entries_' || to_char(m, 'YYYYMM'), m, (m + interval '1 month')::date);
  END LOOP;
END $$;
SELECT wallet_ensure_partitions(3);

-- Gifts sent BY users on this shard.
CREATE TABLE gift_sends (
  id           uuid PRIMARY KEY,
  sender_id    uuid NOT NULL,
  receiver_id  uuid NOT NULL,
  room_id      text NOT NULL DEFAULT '',
  video_id     text NOT NULL DEFAULT '',
  gift_id      text NOT NULL,
  qty          int NOT NULL CHECK (qty > 0),
  coins        bigint NOT NULL CHECK (coins > 0),
  diamonds     bigint NOT NULL CHECK (diamonds >= 0),
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX gift_sends_sender ON gift_sends (sender_id, created_at DESC);

-- Gifts credited TO users on this shard (dedupe for the credit consumer).
CREATE TABLE gift_credits (
  gift_send_id  uuid PRIMARY KEY,
  receiver_id   uuid NOT NULL,
  diamonds      bigint NOT NULL,
  credited_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX gift_credits_at ON gift_credits (credited_at);

-- Catalog (read from the first shard; same rows everywhere is fine).
CREATE TABLE gifts (
  id      text PRIMARY KEY,
  name    text NOT NULL,
  coins   int NOT NULL CHECK (coins > 0),
  sort    int NOT NULL DEFAULT 0,
  active  boolean NOT NULL DEFAULT true
);
INSERT INTO gifts (id, name, coins, sort) VALUES
  ('heart', 'Heart', 1, 1), ('rose', 'Rose', 10, 2), ('star', 'Star', 99, 3),
  ('mic_drop', 'Mic Drop', 299, 4), ('flame', 'Flame', 599, 5), ('diamond', 'Diamond', 999, 6),
  ('crown', 'Crown', 2999, 7), ('rocket', 'Rocket', 9999, 8);

CREATE TABLE coin_packs (
  id                 text PRIMARY KEY,
  coins              bigint NOT NULL CHECK (coins > 0),
  google_product_id  text UNIQUE,
  apple_product_id   text UNIQUE,
  web_amount_kobo    bigint,          -- naira price on the web wallet (kobo)
  sort               int NOT NULL DEFAULT 0,
  active             boolean NOT NULL DEFAULT true
);
-- Prices are placeholders: set real store products and naira prices before launch.
INSERT INTO coin_packs (id, coins, google_product_id, apple_product_id, sort) VALUES
  ('coins_70', 70, 'coins_70', 'app.lumo.coins70', 1),
  ('coins_350', 350, 'coins_350', 'app.lumo.coins350', 2),
  ('coins_700', 700, 'coins_700', 'app.lumo.coins700', 3),
  ('coins_1400', 1400, 'coins_1400', 'app.lumo.coins1400', 4),
  ('coins_3500', 3500, 'coins_3500', 'app.lumo.coins3500', 5),
  ('coins_7000', 7000, 'coins_7000', 'app.lumo.coins7000', 6);

-- Purchases credited to users on this shard.
CREATE TABLE purchases (
  id          uuid PRIMARY KEY,
  user_id     uuid NOT NULL,
  channel     text NOT NULL CHECK (channel IN ('google_play', 'app_store', 'paystack')),
  product_id  text NOT NULL,
  store_ref   text NOT NULL,
  coins       bigint NOT NULL,
  status      text NOT NULL DEFAULT 'credited' CHECK (status IN ('credited', 'reversed')),
  raw         jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (channel, store_ref)
);
CREATE INDEX purchases_user ON purchases (user_id, created_at DESC);

-- Global one-time claim of a store receipt. Lives on the shard chosen by
-- hash(channel:store_ref), so a receipt can be claimed once across ALL shards.
CREATE TABLE purchase_claims (
  claim_key   text PRIMARY KEY,
  user_id     uuid NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE payouts (
  id          uuid PRIMARY KEY,
  user_id     uuid NOT NULL,
  diamonds    bigint NOT NULL CHECK (diamonds > 0),
  status      text NOT NULL DEFAULT 'pending_review'
                CHECK (status IN ('pending_review', 'approved', 'paid', 'rejected')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payouts_user ON payouts (user_id, created_at DESC);
CREATE INDEX payouts_status ON payouts (status, created_at);

CREATE TABLE outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox_lease (
  name text PRIMARY KEY,
  holder text NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE TABLE processed_events (
  consumer text NOT NULL,
  event_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_at ON processed_events (processed_at);
