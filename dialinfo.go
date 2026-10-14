package kafka

import "context"

// DialInfo says why a connection is being opened or re-authenticated. The client attaches it to the context it
// passes to Dialer.DialFunc, Transport.Dial and sasl.Mechanism.Start, where DialInfoFromContext reads it.
type DialInfo struct {
	// Purpose is what needs the connection: partition_reader, group_coordinator, transport or reauth.
	Purpose string
	// Stage is the step of Purpose that dials: lookup (bootstrap metadata) or leader for a partition, find or
	// connect for a group coordinator.
	Stage     string
	GroupID   string
	Topic     string
	Partition int
	// Attempt counts the consecutive failed attempts before this one; 0 is a first try or one after a reset.
	Attempt int
	// ReaderVersion counts how many times the Reader restarted its partition readers, once per rebalance.
	ReaderVersion int64
}

type dialInfoKey struct{}

func ContextWithDialInfo(ctx context.Context, info DialInfo) context.Context {
	return context.WithValue(ctx, dialInfoKey{}, info)
}

func DialInfoFromContext(ctx context.Context) (DialInfo, bool) {
	info, ok := ctx.Value(dialInfoKey{}).(DialInfo)
	return info, ok
}

func withDialStage(ctx context.Context, stage string) context.Context {
	info, _ := DialInfoFromContext(ctx)
	info.Stage = stage
	return ContextWithDialInfo(ctx, info)
}

func withDialPurposeDefault(ctx context.Context, purpose string) context.Context {
	info, _ := DialInfoFromContext(ctx)
	if info.Purpose != "" {
		return ctx
	}
	info.Purpose = purpose
	return ContextWithDialInfo(ctx, info)
}
