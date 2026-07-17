package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// GeoLocation represents the geographical location of an IP address
type GeoLocation struct {
	IP          string  `json:"query"`
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"region"`
	RegionName  string  `json:"regionName"`
	City        string  `json:"city"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	ISP         string  `json:"isp"`
	AS          string  `json:"as"`
}

// String returns a formatted string representation of the geolocation
func (g *GeoLocation) String() string {
	if g.City != "" && g.Country != "" {
		return fmt.Sprintf("%s, %s (%s)", g.City, g.Country, g.CountryCode)
	} else if g.Country != "" {
		return fmt.Sprintf("%s (%s)", g.Country, g.CountryCode)
	}
	return "Unknown"
}

// GeoLocator handles IP geolocation lookups with caching
type GeoLocator struct {
	cache      map[string]*GeoLocation
	cacheMutex sync.RWMutex
	httpClient *http.Client
}

// NewGeoLocator creates a new GeoLocator instance
func NewGeoLocator() *GeoLocator {
	return &GeoLocator{
		cache: make(map[string]*GeoLocation),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// ExtractIPFromMultiaddr extracts the IP address from a multiaddr
func ExtractIPFromMultiaddr(maddr ma.Multiaddr) (string, error) {
	// Try to convert the multiaddr to a net.Addr
	netAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return "", err
	}

	// Extract IP from the net.Addr
	switch addr := netAddr.(type) {
	case *net.TCPAddr:
		return addr.IP.String(), nil
	case *net.UDPAddr:
		return addr.IP.String(), nil
	}

	// Fallback: parse multiaddr string directly
	parts := strings.Split(maddr.String(), "/")
	for i, part := range parts {
		if (part == "ip4" || part == "ip6") && i+1 < len(parts) {
			return parts[i+1], nil
		}
	}

	return "", fmt.Errorf("could not extract IP from multiaddr: %s", maddr.String())
}

// Lookup performs a geolocation lookup for the given IP address
// Results are cached to minimize API calls
func (gl *GeoLocator) Lookup(ctx context.Context, ip string) (*GeoLocation, error) {
	// Check if IP is private/local
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	}

	// Don't lookup private/local IPs
	if parsedIP.IsPrivate() || parsedIP.IsLoopback() || parsedIP.IsLinkLocalUnicast() {
		return &GeoLocation{
			IP:      ip,
			Country: "Local/Private",
		}, nil
	}

	// Check cache first
	gl.cacheMutex.RLock()
	if cached, ok := gl.cache[ip]; ok {
		gl.cacheMutex.RUnlock()
		return cached, nil
	}
	gl.cacheMutex.RUnlock()

	// Make API request
	url := fmt.Sprintf("http://ip-api.com/json/%s?fields=status,message,country,countryCode,region,regionName,city,lat,lon,isp,as,query", ip)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := gl.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("geolocation API returned status %d", resp.StatusCode)
	}

	var result struct {
		Status  string  `json:"status"`
		Message string  `json:"message"`
		GeoLocation
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("geolocation lookup failed: %s", result.Message)
	}

	// Cache the result
	gl.cacheMutex.Lock()
	gl.cache[ip] = &result.GeoLocation
	gl.cacheMutex.Unlock()

	return &result.GeoLocation, nil
}

// LookupMultiaddr extracts IP from multiaddr and performs geolocation lookup
func (gl *GeoLocator) LookupMultiaddr(ctx context.Context, maddr ma.Multiaddr) (*GeoLocation, error) {
	ip, err := ExtractIPFromMultiaddr(maddr)
	if err != nil {
		return nil, err
	}
	return gl.Lookup(ctx, ip)
}
