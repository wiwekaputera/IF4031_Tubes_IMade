package auth

// Scope defines the access scope of a token, which determines the level of access granted to the client.
type Scope string

const (
	ScopeMedia Scope = "media"
	ScopeFieldTeam Scope = "field_team"
	ScopeInternalOps Scope = "internal_ops"
)

const (
	GrantTypeClientCredentials = "client_credentials"
	GrantTypeRefreshToken      = "refresh_token"

	TokenTypeBearer = "Bearer"

	DefaultIssuer   = "bnpb-token-service"
	DefaultAudience = "bnpb-client-api"
	SigningAlgES256 = "ES256"
)

// TokenResponse represents the response from the token service when requesting an access token.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        Scope  `json:"scope"`
}

// TokenClaims represents the claims contained in a JWT access token.
type OAuthError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

const (
	ErrInvalidRequest       = "invalid_request"
	ErrInvalidClient        = "invalid_client"
	ErrInvalidGrant         = "invalid_grant"
	ErrUnauthorizedClient   = "unauthorized_client"
	ErrUnsupportedGrantType = "unsupported_grant_type"
	ErrInvalidScope         = "invalid_scope"
	ErrInsufficientScope    = "insufficient_scope"
	ErrInvalidToken         = "invalid_token"
)
