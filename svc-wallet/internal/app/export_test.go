package app

import (
	"context"

	"github.com/oviethebaddestcoder/lumo-backend/svc-wallet/internal/stores"
)

// Paystack_testCredit runs the real claim + credit path with a fake
// verified receipt (the store API call is the only thing skipped).
func (s *Service) Paystack_testCredit(ctx context.Context, userID, ref string) error {
	pack, err := s.packBy(ctx, "web", "coins_70")
	if err != nil {
		return err
	}
	_, err = s.credit(ctx, userID, stores.Verified{Channel: "paystack", ProductID: pack.ID, StoreRef: ref}, pack)
	return err
}
