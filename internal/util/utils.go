package util

import (
	"os"

	"sigs.k8s.io/yaml"
)

func ArrayContains[T comparable](searchTerms []T, list []T) bool {
	for _, term := range searchTerms {
		found := false
		for _, item := range list {
			if term == item {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

func LoadTestEnv() (map[string]string, error) {
	fdata, err := os.ReadFile(os.Getenv("DELTASTREAM_CRED_FILE"))
	if err != nil {
		return nil, err
	}
	creds := map[string]string{}
	if err = yaml.Unmarshal(fdata, &creds); err != nil {
		return nil, err
	}
	os.Setenv("DELTASTREAM_API_KEY", creds["token"])
	os.Setenv("DELTASTREAM_ORGANIZATION", creds["org"])
	os.Setenv("DELTASTREAM_ROLE", creds["role"])
	os.Setenv("DELTASTREAM_SERVER", creds["server"]+"/v2")
	// os.Setenv("DELTASTREAM_DEBUG", "1")
	return creds, err
}
