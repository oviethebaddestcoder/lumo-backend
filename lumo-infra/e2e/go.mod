module github.com/oviethebaddestcoder/lumo-backend/lumo-infra/e2e

go 1.26.0

require (
	github.com/oviethebaddestcoder/lumo-backend/lumo-contracts v0.0.0
	github.com/twmb/franz-go v1.22.3
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20261109062748-d6ea02a174bc
)

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
)

replace github.com/oviethebaddestcoder/lumo-backend/lumo-contracts => ../../lumo-contracts
