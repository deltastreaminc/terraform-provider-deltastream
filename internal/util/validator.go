// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

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

var UUIDValidators = []validator.String{stringvalidator.RegexMatches(
	regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[4][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`),
	"must contain only alphanumeric characters, space, - and _",
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
			LogError(ctx, resp.Diagnostics, fmt.Sprintf("%s is not a valid uri", u), err)
		}
	}
}
