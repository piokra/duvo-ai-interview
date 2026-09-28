.PHONY: fmt validate plan apply deploy destroy

TF_IMAGE ?= hashicorp/terraform:1.9
TF_DIR := $(CURDIR)/infra/terraform

fmt:
	docker run --rm -v $(TF_DIR):/work -w /work $(TF_IMAGE) fmt -check

validate:
	docker run --rm -v $(TF_DIR):/work -w /work $(TF_IMAGE) init -backend=false
	docker run --rm -v $(TF_DIR):/work -w /work $(TF_IMAGE) validate

plan:
	docker run --rm -e HCLOUD_TOKEN -v $(TF_DIR):/work -v /home/linux/.ssh:/keys:ro -w /work $(TF_IMAGE) plan -var ssh_public_key_path=/keys/id_ed25519.pub

apply:
	docker run --rm -e HCLOUD_TOKEN -v $(TF_DIR):/work -v /home/linux/.ssh:/keys:ro -w /work $(TF_IMAGE) apply -auto-approve -var ssh_public_key_path=/keys/id_ed25519.pub

deploy:
	./scripts/deploy.sh "$$(docker run --rm -v $(TF_DIR):/work -w /work $(TF_IMAGE) output -raw server_ip)"

destroy:
	docker run --rm -e HCLOUD_TOKEN -v $(TF_DIR):/work -v /home/linux/.ssh:/keys:ro -w /work $(TF_IMAGE) destroy -var ssh_public_key_path=/keys/id_ed25519.pub
