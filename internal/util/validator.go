package util

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

var IdentifierValidators = []validator.String{stringvalidator.RegexMatches(
	regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_@]*$`),
	"must contain only alphanumeric characters, _ and @",
)}

type UrlsValidator struct{}

func (v UrlsValidator) Description(ctx context.Context) string {
	return "validates a comma seperated string of URIs"
}

func (v UrlsValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v UrlsValidator) ValidateString(ctx context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsUnknown() || req.ConfigValue.IsNull() {
		return
	}

	s := req.ConfigValue.ValueString()
	uris := strings.Split(s, ",")
	for _, u := range uris {
		_, err := url.Parse(u)
		if err != nil {
			resp.Diagnostics.AddError("invalid uri", fmt.Sprintf("%s is not a valid uri: %s", u, err.Error()))
		}
	}
}
