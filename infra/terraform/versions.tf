# Terraform + provider pins for Stream2Pretrain on DHBWCloud OpenStack.
# OpenStack provider 3.x is the latest 3.0+ line and works with Yoga/Bobcat clouds.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.0"
    }
    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = "~> 2.3"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "openstack" {
  # Reads OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_PROJECT_NAME, OS_USER_DOMAIN_NAME,
  # OS_PROJECT_DOMAIN_NAME, OS_REGION_NAME from environment (clouds.yaml or `source openrc.sh`).
  # Do not hardcode credentials.
}
