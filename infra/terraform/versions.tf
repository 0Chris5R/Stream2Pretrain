# Terraform + provider pins for Stream2Pretrain on DHBWCloud OpenStack.
# This mirrors the working demo setup: OpenStack creates the VMs and the local
# provider writes the generated Ansible inventory.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

provider "openstack" {
  # Reads OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_PROJECT_NAME, OS_USER_DOMAIN_NAME,
  # OS_PROJECT_DOMAIN_NAME, OS_REGION_NAME from environment (clouds.yaml or `source openrc.sh`).
  # Do not hardcode credentials.
}
