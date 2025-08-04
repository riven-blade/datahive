// Package server provides a unified server framework for network communication.
// Use protocol package directly for message types and business data structures.
package server

func CreateGNetServer(address string, cfg *Config) (Transport, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	transport := NewGNetTransport(address, cfg)
	return transport, nil
}
