default: fmt doc
	go install .

.PHONY: fmt
fmt:
	terraform fmt -recursive ./examples/

.PHONY: doc
doc:
	go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs generate -provider-name deltastream

.PHONY: testacc
testacc:
	cat "test-env.yaml" | tr -d '[:space:]' | base64 -d > "$(PWD)/test-env-decoded.yaml"
	DELTASTREAM_SESSION_ID=RANDOM TF_LOG=info TF_ACC=1  DELTASTREAM_CRED_FILE=$(PWD)/test-env-decoded.yaml go test ./... -v $(TESTARGS) -timeout 120m

