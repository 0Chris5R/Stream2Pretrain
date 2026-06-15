<div class="lecturetitle">Immutable Infrastructure</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Mutable Infrastructure

Traditional update path for applications
- Infrastructure is created once <comment>(bare metal, virtual machine, ...)</comment>
- OS and software gets installed and updated regularly

<img src='img/infrastructure-mutable.svg' style='width: 95%; margin-left: 20px;'>

No changes to the underlying infrastructure 
- I.e., constant infrastructure but changing software

---
## Mutable Infrastructure

Issues with mutable infrastructure
- Errors may occur during the update process 
- Leaves the system in an undefined state between to versions

In databases, this requires a rollback to the previous state
- Requires advanced mechanisms to rollback a server's state
- E.g., snapshots before installation of updates

Idea: version infrastructure and application 
- So-called immutable infrastructure

---
## Immutable Infrastructure

No changes to infrastructure after initial deployment

> What is Immutable Infrastructure? A system that does not change once it has been deployed.
> [Timothy Gerla (Co-founder and CTO at Ansible), 2020](https://www.cncf.io/webinars/immutable-infrastructure-in-the-age-of-kubernetes/)

Updates: fresh infrastructure and installation from scratch

<img src='img/infrastructure-immutable.svg' style='width: 60%; margin-left: 3rem;'>

---
## Immutable Infrastructure: Properties

No In-Place Changes
- Once deployed, no changes occur <comment>(updates and modifications require a new version)</comment>
- Consequence: versioned artifacts

Consistent Environments
- Each deployment is based on a fixed image or configuration
- Environments <comment>(dev, test, production)</comment> are reproducible
- Infrastructure and application builds are automated 

Updates require fresh infrastructure
- Manual installation of hardware tedious and time-consuming
- Requires automated infrastructure provisioning using IaaS


---
## Immutable Infrastructure: Properties

Simplified Operations
- Rollbacks: Redeploy the previous immutable artifact
- No configuration drift <comment>(servers deviate from intended state over time)</comment>

Enhanced Security
- Servers can be treated as disposable
- If compromised, they can be easily replaced with a fresh, secure version

Infrastructure becomes part of the application's version
- Applications describe the required infrastructure
- Description included in the software's code repository
- Consequence: Infrastructure as Code

---
## Immutable Infrastructure: Challenges

Build and Deployment Complexity
- Requires robust build automation and deployment pipelines
- May involve significant investment in tooling and process changes

Storage and Cost
- Immutable artifacts <comment>(e.g., container images, VM snapshots)</comment> can consume significant storage
- Managing and storing multiple versions of artifacts can increase costs

State Management
- Immutable infrastructure typically suits stateless applications
- Handling stateful applications can be challenging

---
## Issue With Stateful Applications
<!-- .slide: data-name="Stateful Applications" -->

Applications keep state in the file system
- Logs, databases, uploaded files, ...
- Replacing the VM discards this state unless it is explicitly managed

Strategies for handling state

| Strategy           | Description                                                                                                                                             | Tradeoffs                                                   |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| External storage   | Attach a persistent volume <comment>(iSCSI LUN, [CEPH](https://ceph.io/), [AWS EBS](https://aws.amazon.com/ebs/))</comment>; reattach after replacement | Stateless VM, but storage must be managed separately        |
| External database  | Move all persistent state to a dedicated database server outside the VM                                                                                 | Clean separation; the VM itself becomes truly stateless     |
| VM snapshots       | Take a snapshot before replacing; restore on rollback                                                                                                   | Fast recovery, but snapshots accumulate and drift over time |
| Backup and restore | Periodic backup of state <comment>(e.g., database dumps, file backups)</comment>                                                                        | Simple; acceptable data loss risk depending on interval     |

---
## Infrastructure as Code
<!-- .slide: data-name="Infrastructure as Code" -->

Comprises a collection of <comment>(potentially many)</comment>...
- Compute instances
- Different applications

Challenges
- 1\.) Deploy and operate <comment>(enough)</comment> compute instances
- 2\.) Install applications on compute instances

<img src='img/virtualization-0-mapping-applications-to-compute-instances.svg' style='width: 90%;'>

---
## Challenge: Deploy Compute Instances

Application updates &rarr; updates to compute infrastructure
- Manual installation of physical compute infrastructure not sensible
- Automated management of compute infrastructure <comment>(terraforming)</comment>

IaaS providers offer APIs for programmatic management
- Example: Creation of compute infrastructure <comment>(virtual machines, VMs)</comment>
- [AWS EC2](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/Welcome.html): [RunInstances](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_RunInstances.html) and [TerminateInstances](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_TerminateInstances.html)
- OpenStack: [Create Server](https://docs.openstack.org/api-ref/compute/?expanded=create-server-detail#create-server) and [Delete Server](https://docs.openstack.org/api-ref/compute/?expanded=create-server-detail#delete-server)

---
## Example: AWS EC2 and OpenStack API

Create 3 machines with [AMI](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/finding-an-ami.html) `ami-60a54009` with HTTP GET

```url
https://ec2.amazonaws.com/?Action=RunInstances
  &ImageId=ami-60a54009&MaxCount=3&MinCount=1&KeyName=my-key
  &Placement.AvailabilityZone=us-east-1d&AUTHPARAMS
```

HTTP POST to `http://openstack-controller/compute/v2.1/servers` with JSON content

```json
{
  "server" : {
        "name" : "new-server-test",
        "imageRef" : "70a599e0-31e7-49b7-b260-868f441e862b",
        "flavorRef" : "m1.small",
        "key_name" : "Dennis Mac 2017",
        "availability_zone": "nova",
        "security_groups": [{"name": "default"}],
    }
}
```

<credits>
  AWS example taken from <a href="https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_RunInstances.html#API_RunInstances_Examples">AWS EC2 API Reference</a>
</credits>

---
## Terraforming Tools

Different tools for different needs and preferences

| Tool                                                                                 | Scope          | Language            | Notes                                                                   |
| ------------------------------------------------------------------------------------ | -------------- | ------------------- | ----------------------------------------------------------------------- |
| [CloudFormation](https://aws.amazon.com/cloudformation/)                             | AWS only       | JSON/YAML           | Declarative; tightly coupled to AWS                                     |
| [Azure Bicep](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/) | Azure only     | Bicep DSL           | Successor to ARM templates                                              |
| [OpenStackClients](https://wiki.openstack.org/wiki/OpenStackClients)                 | OpenStack only | Python/CLI          | CLI and SDK for OpenStack APIs                                          |
| [Pulumi](https://www.pulumi.com/)                                                    | Multi-provider | Python, Go, TS, ... | IaC in general-purpose languages                                        |
| [Ansible](https://www.ansible.com/)                                                  | Multi-provider | YAML                | Primarily configuration management; also provisions cloud resources     |
| [Crossplane](https://www.crossplane.io/)                                             | Multi-provider | Kubernetes CRDs     | Manages cloud infrastructure via Kubernetes control loop; covered later |
| [Terraform](https://www.terraform.io/)                                               | Multi-provider | HCL                 | Original; large provider ecosystem; BSL license since v1.6              |
| [OpenTofu](https://opentofu.org/)                                                    | Multi-provider | HCL                 | Open-source fork of Terraform; maintained by Linux Foundation           |

---
## Demo: Terraforming using Ansible

<video controls defer data-autoplay style="width: 100%;"> <!-- loop --> 
  <source src="img/openstack-ansible-compute-instance-creation.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>


---
# Hands-On: IaaS Automation using Terraform
<!-- .slide: data-name="Terraform" -->

---
## IaaS DevOps: Terraform

Tool for building, changing, and versioning infrastructure
- Supports variety of IaaS service providers, cf. [Major Cloud Providers](https://www.terraform.io/docs/providers/type/major-index.html) and [Cloud Providers](https://www.terraform.io/docs/providers/type/cloud-index.html) for supported providers
- Configuration files describe required infrastructure for applications
- Configuration changes yield incremental execution plans

Key features
- Infrastructure as code <comment>(high-level configuration syntax, versioned just like code, can be shared and re-used)</comment>
- Execution Plans <comment>(planning step before applying it)</comment>
- Resource Graph <comment>(allows parallelization of any non-dependent actions)</comment>
- Change Automation <comment>(complex changesets with minimal human interaction)</comment>

---
## Terraform Configuration

Each project is stored in its own directory
- Contains one or more configuration file(s) in a directory
- File suffix `.tf` 

Terraform files may contain
- A list of required providers <comment>(e.g., OpenStack provider)</comment>
- Provider-specific configuration <comment>(e.g., EDSC Mannheim)</comment>
- Resources definitions <comment>(e.g., virtual machines, networks, ...)</comment>

---
## Provider Requirements Configuration

Contains required providers and <comment>(optionally)</comment> their versions
- Providers <comment>(and their versions)</comment> are listed in the [Terraform Registry](https://registry.terraform.io/browse/providers)
- Syntax for [provider requirements](https://www.terraform.io/docs/configuration/provider-requirements.html) and [acceptable version definition](https://www.terraform.io/docs/configuration/version-constraints.html)
- Optional: provider version (`version = "~> 1.33.0"` allows increasing most specific segment only &rarr; `>= 1.33.0, < 1.34`)

Example (providers [local](https://registry.terraform.io/providers/hashicorp/local/latest) and [openstack](https://registry.terraform.io/providers/terraform-provider-openstack/openstack/latest))

<a data-code='terraform' data-link href="code/terraform-openstack/versions.tf">Source code</a>

---
## Provider Configuration

Each provider can/must be configured
- This is specific to each provider <comment>(requires reading its documentation)</comment>

```terraform
provider "some-provider" {
 # Provider-specific configuration details
}
# More provider configurations...
```

Example: OpenStack provider
- Requires authentication information, URL of the controller, ...
- For details, cf. [OpenStack Provider](https://registry.terraform.io/providers/terraform-provider-openstack/openstack/latest/docs)

```terraform
provider "openstack" {
  user_name   = "admin"
  password    = "pwd"
  auth_url    = "http://myauthurl:5000/v2.0"
  # Others ...
}
```

---
## Terraform Configuration: Resources

[Resource](https://www.terraform.io/docs/configuration/resources.html) definitions are comprised of a type, a local name, and configuration of the resource

```terraform
resource "type" "internal_name" {
 # Resource-specific configuration details
}
```

Example creating an OpenStack compute instance 
- Type [`openstack_compute_instance_v2`](https://registry.terraform.io/providers/terraform-provider-openstack/openstack/latest/docs/resources/compute_instance_v2)
- Local name `my_web_server`
- Configuration of name, image_id, flavor_name, and key_pair

```terraform
resource "openstack_compute_instance_v2" "my_web_server" {
  name = "name_in_openstack"
  image_id = "a0a1c616-f4f3-429d-8de9-8e74b5df805c"
  flavor_name = "m1.small"
  key_pair = "my_key_pair_name"
}
```

---
## Terraform CLI

Initialize project: `terraform init`
- Downloads required modules, etc. into directory `.terraform`
- Re-run after adding, modifying, or removing providers

Show terraform state or plan
- Show execution plan: `terraform plan`
- Display last known state: `terraform show` <comment>(stored in `terraform.tfstate`)</comment>

Build (or change) infrastructure according to configuration
- Run `terraform apply` repeatedly

Destroy infrastructure
- Run `terraform destroy`

---
## Terraform Exercise

<a data-exercise="terraform">Terraform exercise</a>

<!--
<asciinema data-conf='{ "cols": 120, "rows": 25, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "16px"}'
        src="img/terraform-demo.cast" />
-->