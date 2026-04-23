// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package utils

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ExplainRemoteRepositoryOpenError returns a short, user-oriented message for a failed
// OpenRemoteRepository. If includeDetails is true, the caller may also print the raw error.
func ExplainRemoteRepositoryOpenError(target string, err error) (friendly string, includeDetails bool) {
	if err == nil {
		return "", false
	}
	raw := err.Error()
	lower := strings.ToLower(raw)

	switch {
	case strings.Contains(lower, "produced zero addresses") ||
		strings.Contains(lower, "name resolver error") ||
		strings.Contains(lower, "no such host"):
		return fmt.Sprintf(
			"The remote repository server '%s' does not exist or cannot be found (host name could not be resolved). Check the spelling and your network.",
			target,
		), false

	case strings.Contains(lower, "connection refused"):
		return fmt.Sprintf(
			"Could not connect to '%s' (connection refused). Check the host, port, and that an SST repository service is listening there.",
			target,
		), false

	case strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "timeout"):
		return fmt.Sprintf(
			"Connecting to '%s' timed out. Check your network, VPN, and firewall.",
			target,
		), true

	case strings.Contains(lower, "certificate") ||
		strings.Contains(lower, "tls ") ||
		strings.Contains(lower, "x509"):
		return fmt.Sprintf(
			"TLS handshake failed for '%s'. The certificate may be invalid or not trusted.",
			target,
		), true
	}

	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			return fmt.Sprintf("Authentication was rejected for '%s'. Check your username and password.", target), true
		case codes.NotFound:
			return fmt.Sprintf("The repository was not found on the server at '%s'.", target), false
		case codes.Unavailable:
			return fmt.Sprintf("The server at '%s' is unavailable. It may be down or the address may be wrong.", target), true
		}
	}

	return fmt.Sprintf("Could not open the remote repository at '%s'.", target), true
}
