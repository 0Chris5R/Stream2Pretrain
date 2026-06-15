<div class="lecturetitle">Configuration Management</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Configuration Management

Configuration management
- Goal: replicate software and applications across many hosts
- Includes Software Configuration Management (i.e., revision control)
- Prepare infrastructure (virtual machines, software, firewalls, etc.)
- Configuration software (dependencies, databases, web servers, etc.)
- Covers changes typically made by a system administrator

Goal: automate configuration management
- Reduce manual work and errors
- Verify that the desired state (hosts, software, ...) is achieved
- Apply changes to many hosts at once

---
## Challenge: Install Apps on VMs

Once infrastructure is available, apps are deployed
- Requires mapping of apps to virtual machines

Different deployment strategies
- Multiple Apps per VM
- Single App per VM

---
## Multiple Apps Per VM Strategy

Multiple Applications per compute instance
- Good use of resources
- Low maintenance requirements <comment>(OS security patches, ...)</comment>

<img src='img/virtualization-1-multiple-applications-per-compute-instance.svg' 
     style='padding-left: 20px; width: 80%;'>

Cons
- Conflicting requirements <comment>(i.e., different library versions, etc.)</comment>
- Development &rarr; operations is difficult <comment>(it worked on my machine)</comment>

---
## Single App Per VM Strategy

Isolation on hardware level
- Well-defined run-time environment per application

<img src='img/virtualization-2-single-app-single-machine.svg' 
     style='padding-left: 20px; width: 80%;'>

Cons
- Waste of resources <comment>(CPU utilization, money, ...)</comment>
- Operating systems nearly identical <comment>(e.g., Debian/Ubuntu or Windows)</comment>
- Overhead <comment>(installation, startup time, security updates, backup, etc.)</comment>

---
## Tools for Configuration Management

Tools to automate app installation (configuration management)
- [Vagrant](https://www.vagrantup.com/)
- [Amazon AWS Cloudformation](https://aws.amazon.com/cloudformation/)
- [Puppet](https://puppet.com/)
- [Chef](https://www.chef.io/)
- [Ansible](https://www.ansible.com/) (and UI: [Ansible Semaphore](https://www.ansible-semaphore.com/))

Others: [Comparison of open-source configuration management software](https://en.wikipedia.org/wiki/Comparison_of_open-source_configuration_management_software)

---
## IaaS Automation: Ansible
<!-- .slide: data-name="Ansible" -->

Ansible vs. Terraform
- Terraform focuses on infrastructure provisioning
- Ansible's focuses on creating a certain (software) state on machines
- Ansible requires SSH and Python on the remote machine

Playbooks describe the desired state
- Uses the [YAML](https://learnxinyminutes.com/docs/yaml/) file format
- Many [modules](https://docs.ansible.com/ansible/latest/collections/index.html#list-of-collections) for a variety of tasks
- Packaging modules install software packages (e.g., using [apt](https://docs.ansible.com/ansible/latest/collections/ansible/builtin/apt_module.html#ansible-collections-ansible-builtin-apt-module), [pip](https://docs.ansible.com/ansible/latest/collections/ansible/builtin/pip_module.html#ansible-collections-ansible-builtin-pip-module))
- Cloud modules can provision infrastructure (e.g., on [AWS](https://docs.ansible.com/ansible/latest/collections/amazon/aws/index.html#plugins-in-amazon-aws))

---
## YAML Primer

<a data-code='yaml' href="code/yaml-primer/yaml-primer.yaml">Source code</a>

---
## Ansible: Set-Actual Comparison

Playbooks describe the desired state
- Ansible tries to minimize changes on remote hosts

It therefore performs a set-actual comparison
- Each module gathers the current state (actual)
- Compares this with the actual state
- Performs required actions to achieve the desired state

Ansible implicitly gathers some facts
- E.g. CPUs, memory, network interfaces, ...
- See [Gathers facts about remote hosts](https://docs.ansible.com/ansible/latest/collections/ansible/builtin/gather_facts_module.html) for details

---
## Ansible Playbook: Example

Goal: Install `git` on all machines
- Logs in via SSH as user `ubuntu`
- Uses `sudo` to become root
- Installs `git` (only if missing)

Playbook (`deploy.yml`)

```yaml
- hosts: all # This is a play
  user: ubuntu
  become: yes
  become_user: root

  tasks: # This is a list of tasks
    - name: Install packages # This is a task
      apt: name=git update_cache=yes state=latest
```

---
## Ansible Playbook: Example

List of hosts (file `hosts`)

```
192.168.1.1
```

Run the playbook

```sh
# Run this once per new shell to have sensible defaults
export ANSIBLE_STDOUT_CALLBACK=debug 
export ANSIBLE_HOST_KEY_CHECKING=False 

# Execute the playbook
ansible-playbook -i hosts deploy.yml
```

---
## Structure of Ansible Playbooks

A playbook project typically follows this layout

```console
my-playbook/
├── site.yml                   # Entry point — maps host groups to roles/tasks
├── hosts                      # Inventory: which machines to target
├── requirements.yml           # Role dependencies (ansible-galaxy install -r)
├── tasks/
│   └── main.yml               # Task list included by the playbook
├── handlers/
│   └── main.yml               # Handlers triggered via notify
├── vars/
│   └── main.yml               # Variables for this playbook
├── templates/                 # Jinja2 templates rendered on remote hosts
└── files/                     # Static files copied as-is
```
<!-- .element: style="font-size: 0.5em;" -->

Playbook is the main entry point
- Includes tasks, handlers, variables, templates, and files as needed
- Tasks are executed in order
- Handlers are triggered by tasks and run at the end of the play

---
## Task Example

`tasks/main.yml` 
- List of tasks to execute

```yaml
- name: Install nginx
  apt:
    name: nginx
    state: present

- name: Deploy config file
  template:
    src: nginx.conf.j2
    dest: /etc/nginx/nginx.conf
  notify: Restart nginx          # triggers the handler below
```

---
## Main Playbook

Calls the task file and defines the handler

```yaml
- hosts: webservers
  become: true
  vars_files:
    - vars/main.yml
  tasks:
    - import_tasks: tasks/main.yml
  handlers:
    - name: Restart nginx
      service:
        name: nginx
        state: restarted
```

---
## Ansible Roles
<!-- .slide: data-name="Ansible Roles" -->

Playbooks grow complex as more tasks are added
- Roles package tasks, handlers, variables, templates, and files into a reusable unit
- Roles do one thing well and can be shared across projects or teams
- Published roles are available on [Ansible Galaxy](https://galaxy.ansible.com/)

Standard directory structure (loaded automatically)

```
roles/
└── my-role/
    ├── tasks/
    │   └── main.yml       # Executed when the role is applied
    ├── defaults/
    │   └── main.yml       # Default variable values
    ├── handlers/
    │   └── main.yml       # Handlers triggered by notify
    ├── templates/         # Templates rendered on the remote host
    └── files/             # Static files copied as-is
```
<!-- .element: style="font-size: 0.65em;" -->
---
## Ansible Roles: Usage

Install a role from Ansible Galaxy or a Git repository

```bash
# Install from Ansible Galaxy
ansible-galaxy install geerlingguy.nodejs

# Or declare dependencies and install all at once
ansible-galaxy install -r requirements.yml
```

```yaml
# requirements.yml
- name: k3s-dhbw-cloud-role
  src: https://github.com/pfisterer/k3s-dhbw-cloud-role
  version: main
```

Apply a role in a playbook 
- Variables override the role's defaults

```yaml
- hosts: all
  become: true
  roles:
    - role: geerlingguy.nodejs
      vars:
        nodejs_version: "20.x"
```

---
## Ansible Playbook: Exercise

<a data-exercise="ansible">Ansible exercise</a>

---
## Image Building with Packer
<!-- .slide: data-name="Packer" -->

Terraform and Ansible provision and configure infrastructure
- Often pre-provisioned OS images (e.g., Ubuntu, ...) are used
- Ansible or cloud-init installs packages after the VM starts
- Slow, network-dependent, potentially inconsistent across runs

[Packer](https://www.packer.io/) builds machine images before deployment
- Start temporary VM, run provisioner <comment>(shell scripts, Ansible, ...)</comment>
- Supports AWS AMI, OpenStack Glance, VMware, Docker, ...

Result: fully-configured, tested image that boots into a ready state
- Each step is independently versioned and auditable
- Infrastructure is reproducible at every level

---
## Packer: How It Works

Packer reads a template that defines the build pipeline
- Written in [HCL2](https://developer.hashicorp.com/packer/docs/templates/hcl_templates) <comment>(HashiCorp Configuration Language, cf. Terraform)</comment>
- Three main blocks: sources, provisioners, post-processors

Sources define the target platform and base image
- One source per target <comment>(AWS AMI, OpenStack Glance, Docker, ...)</comment>
- Multiple sources can build in parallel for different platforms

Provisioners configure the running VM
- Shell scripts, Ansible playbooks, Chef, Puppet, ...
- Same tools and playbooks as for live infrastructure

Post-processors transform the resulting artifact
- Compress, upload to registry, create Vagrant box, ...

---
## Packer: Define the Source

Source block defines base image and build environment
- Describes where the temporary VM comes from

```hcl
source "openstack" "ubuntu" {
  # Base image to start from in OpenStack
  source_image_name = "Ubuntu 22.04"   
  # Name of the resulting image in OpenStack
  image_name        = "my-app-v1.0"    
  # Flavor, network, and other settings for the temporary VM
  flavor            = "m1.small"
  ssh_username      = "ubuntu"
  networks          = ["DHBW"]
  security_groups   = ["default", ]
}
```

Packer starts a temporary VM from `source_image_name`
- Waits until SSH is available
- All subsequent steps run inside this VM

---
## Packer: Provision the VM

The `build` block defines what runs inside the temporary VM

```hcl
build {
  # Reference to the source block defined earlier
  sources = ["source.openstack.ubuntu"]

  provisioner "shell" {
    # Install nginx using apt-get
    inline = ["apt-get update", "apt-get install -y nginx"]
  }

  provisioner "ansible" {
     # Same playbook as for live machines
    playbook_file = "deploy.yml"  
  }
}
```

Provisioners run in order, connected via SSH
- Shell scripts for simple steps, Ansible for complex configuration
- Same Ansible playbooks reusable from regular deployments

---
## Packer: Run the Build

The source image is started in OpenStack
- Execute commands inside the VM via SSH to set it up
- Provisioners are run locally (e.g., Ansible)

```sh
# Download required Packer plugins (once)
packer init .

# Run the build
packer build template.pkr.hcl
```

After provisioners finish, Packer snapshots the VM
- Snapshot is used as a new image in OpenStack
- The image is then available for use in Terraform
```hcl
data "openstack_images_image_v2" "app" {
  name = "my-app-v1.0"
}
```

---
## Summary
<!-- .slide: data-name="Summary" -->

IaaS: virtualized compute, storage, and networking on demand
- VMs are created and deleted via API <comment>(OpenStack, AWS, ...)</comment>
- Manual setup works <comment>(not scalable, configuration drift)</comment>

Immutable infrastructure: replaces instead of modifying systems
- No manual changes to live VMs <comment>(new deployment)</comment>
- Infrastructure as Code <comment>(reproducible, version-controlled deployments)</comment>

Configuration management automates what runs inside a VM
- Ansible idempotently describes desired software state via playbooks
- Packer bakes images before deployment

Together these tools form a full automation pipeline
- Each layer is independently versioned, testable, and reproducible

