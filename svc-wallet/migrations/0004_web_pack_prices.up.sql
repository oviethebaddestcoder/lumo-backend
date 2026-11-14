-- Web (Paystack) prices for the coin packs, which shipped without any, so
-- the web wallet had nothing to sell. PLACEHOLDER: ₦20 per coin, close to
-- app-store parity. Set real prices before launch:
--   UPDATE coin_packs SET web_amount_kobo = ... WHERE id = ...;
-- Only fills packs that still have no web price.
UPDATE coin_packs SET web_amount_kobo = coins * 2000 WHERE web_amount_kobo IS NULL;
