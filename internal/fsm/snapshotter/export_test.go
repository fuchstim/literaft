package snapshotter

// EncodeHeaderForTest exposes the unexported snapshot-stream header encoder to
// the external test package, so a test can build a stream with a valid header
// and then exercise Restore's page-parsing validation branches past it.
func EncodeHeaderForTest(index uint64) []byte {
	h := encodeHeader(index)
	return h[:]
}

// HeaderSizeForTest is the fixed header length prepended to every snapshot
// stream.
const HeaderSizeForTest = headerSize
