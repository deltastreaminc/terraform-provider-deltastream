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
	DELTASTREAM_DEBUG=1 DELTASTREAM_SESSION_ID=RANDOM TF_LOG=info TF_ACC=1  DELTASTREAM_CRED_FILE=$(PWD)/test-env.yaml go test -v ./... -v $(TESTARGS) -timeout 120m

