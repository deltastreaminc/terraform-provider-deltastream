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
	aws secretsmanager get-secret-value --secret-id deltastream/terraform-provider-deltastream/git-action-secrets --region us-east-2 --query SecretString --output text > "$(PWD)/test-env2.yaml"
	DELTASTREAM_SESSION_ID=RANDOM TF_LOG=info TF_ACC=1  DELTASTREAM_CRED_FILE=$(PWD)/test-env2.yaml go test ./... -v $(TESTARGS) -timeout 120m

