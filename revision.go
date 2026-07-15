package statecharts

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

const (
	// RevisionEnvelopeVersion identifies the inputs and framing used to derive
	// chart revisions. It must change whenever that material or framing changes.
	RevisionEnvelopeVersion uint32 = 1

	// RevisionEnvelopeMagic separates revision material from every other
	// versioned canonical byte format.
	RevisionEnvelopeMagic = "statecharts-revision\x00"
)

// RevisionID is the deterministic identity of one compiled Chart revision.
// Version 1 IDs are "sha256:" followed by 64 lowercase hexadecimal digits.
type RevisionID string

// deriveRevisionFromCanonical hashes a versioned, length-prefixed envelope
// containing the canonical Definition bytes, datamodel name, and deterministic
// program fingerprint. Canonical definition bytes already contain the explicit
// revision salt and every model expression/function reference, including Go
// host-function names and versions. Runtime pointers, registration order,
// clocks, process IDs, and compiled engine artifacts are therefore excluded.
func deriveRevisionFromCanonical(canonical []byte, datamodel Identifier, fingerprint []byte) RevisionID {
	digest := sha256.New()
	_, _ = digest.Write([]byte(RevisionEnvelopeMagic))
	var version [4]byte
	binary.BigEndian.PutUint32(version[:], RevisionEnvelopeVersion)
	_, _ = digest.Write(version[:])
	writeRevisionField(digest, canonical)
	writeRevisionField(digest, []byte(datamodel))
	writeRevisionField(digest, fingerprint)
	return RevisionID("sha256:" + hex.EncodeToString(digest.Sum(nil)))
}

func writeRevisionField(destination hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write(value)
}
