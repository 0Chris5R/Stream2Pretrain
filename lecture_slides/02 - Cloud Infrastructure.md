<div class="lecturetitle">Infrastructure</div>
<!-- .slide: data-state="hide-menubar" -->

---
## Cloud: Infrastructure as a Service (IaaS)
<!-- .slide: data-name="IaaS" -->

Full virtualization of infrastructure
- API access to physical infrastructure resources
- So-called Hyper-Converged Infrastructure (HCI)

Common services
- Compute <comment>(provision virtual computers)</comment>
- Image library <comment>([operating system images](https://docs.openstack.org/image-guide/obtain-images.html))</comment>
- Block storage <comment>(fixed-sized chunks of storage, basis for file systems)</comment>
- File and object storage <comment>(one layer above file systems)</comment>
- Networking <comment>(firewall, load balancing, IP addresses, VLANs, etc.)</comment>

---
## Compute Service

Provides computing instances comprised of
- Virtual CPU(s)
- Memory (RAM)
- (Ephemeral) disks and storage volumes
- Networking

Typically provided using virtualization techniques
- Allows multiple operating systems to simultaneously share processor resources (mostly x86/amd64 based)
- Requires virtualization management

Core technology: virtual machines

---
## Virtual Machines
<!-- .slide: data-name="Virtualization" -->

Virtualized computing instances
- Virtual machines share underlying physical resources
- Run multiple, isolated operating systems simultaneously

<img src="https://upload.wikimedia.org/wikipedia/commons/1/1a/VMM-Type2.JPG" style="width: 40%; padding-left: 20px;">

Required software: Hypervisor
- Examples: VMware, VirtualBox, MS Hyper-V, ...

<credits>Image source: <https://de.wikipedia.org/wiki/Datei:VMM-Type2.JPG></credits>

---
## Demo: Virtualization using a Hypervisor

<video controls defer data-autoplay style="width: 100%; "> <!-- loop -->
  <source src="img/starting-vms-on-macos.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>

---
## Cloud Hypervisors

Cloud providers use _bare-metal hypervisors_
- Examples: Linux KVM, MS Hyper-V, and VMware ESX/ESXi

<img src="https://upload.wikimedia.org/wikipedia/commons/5/53/VMM-Type1.JPG" style="width: 48%; padding-left: 20px;">

Provide web-based interfaces and APIs
- Allows clients to create/destroy virtual machines
- APIs provide programmatic access

<credits><a href="https://de.wikipedia.org/wiki/Datei:VMM-Type1.JPG">Image source</a></credits>

---
## IaaS Providers: Hyperscalers
<!-- .slide: data-name="IaaS Providers" -->

Three dominant public cloud providers ([market share ~70%](https://www.statista.com/chart/18819/worldwide-market-share-of-leading-cloud-infrastructure-service-providers/))
- [Amazon EC2](https://aws.amazon.com/ec2), [Microsoft Azure](https://azure.microsoft.com/), and [Google Compute Engine](https://cloud.google.com/compute/)

Pricing Models

|                    |                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| On-demand          | Pay per second/hour, no commitment <br> <comment>(unpredictable or short-lived workloads)</comment>                        |
| Reserved instances | 1–3 year commitment <br> <comment>(up to ~70% discount, stable, long-running workloads)</comment>                          |
| Spot / Preemptible | Spare capacity at up to ~90% discount <br> <comment>(can be interrupted; fault-tolerant batch jobs, ML training)</comment> |
| Savings Plans      | Commit to fixed hourly spend <br> <comment>(not instance-specific like reserved)</comment>                                 |

<!-- .element: style="margin-left: 20px; font-size: .6em;" -->

Things to watch out for
- Egress costs <comment>(inbound free; outbound billed per GB)</comment>
- Data residency <comment>(regions/availability zones determine storage location)</comment>
- Vendor Lock-In <comment>(legacy APIs, managed services)</comment>

---
## IaaS Providers: HCI

Traditional hyperscalers are public clouds
- You rent compute from someone else's data center

Hyperconverged Infrastructure (HCI) 
- Brings the cloud model on-premise and converges compute, storage, and networking into a single software-defined layer
- Managed as a single system <comment>(not three separate silos)</comment>
- Runs on commodity x86 hardware <comment>(no specialized SAN / NAS)</comment>

Benefits
- Data sovereignty <comment>(regulated industries, government)</comment>
- Latency requirements <comment>(edge, industrial, real-time control)</comment>
- Cost predictability <comment>(no egress fees, no per-second billing surprises)</comment>

---
## IaaS Providers: HCI Products

| Product                                                                                                                | Vendor               | Type       | Notes                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------- | -------------------- | ---------- | ------------------------------------------------------------------------------------------ |
| [VMware Cloud Foundation](https://www.vmware.com/products/cloud-foundation.html)                                       | Broadcom             | Commercial | Enterprise market leader; controversial licensing changes post-Broadcom acquisition (2023) |
| [Nutanix](https://www.nutanix.com/)                                                                                    | Nutanix              | Commercial | Widely deployed; strong hybrid cloud story                                                 |
| [Azure Stack HCI](https://azure.microsoft.com/en-us/products/azure-stack/hci)                                          | Microsoft            | Commercial | Deep Azure integration; relevant for hybrid cloud scenarios                                |
| [Dell EMC VxRail](https://www.delltechnologies.com/en-us/converged-infrastructure/vxrail/index.htm)                    | Dell                 | Commercial | VMware-based (enterprise data centers)                                                     |
| [Cisco HyperFlex](https://www.cisco.com/c/en/us/products/hyperconverged-infrastructure/hyperflex-hx-series/index.html) | Cisco                | Commercial | Integrated with Cisco networking stack                                                     |
| [Proxmox VE](https://www.proxmox.com/)                                                                                 | Proxmox              | OSS        | Popular in education and SME; KVM + LXC                                                    |
| [Harvester](https://harvesterhci.io/)                                                                                  | SUSE                 | OSS        | Kubernetes-native HCI (KubeVirt); bridges HCI and cloud-native                             |
| [Apache CloudStack](http://cloudstack.apache.org/)                                                                     | Apache               | OSS        | Mature; used by cloud service providers                                                    |
| [OpenNebula](https://opennebula.org/)                                                                                  | OpenNebula           | OSS        | Lightweight; edge and hybrid cloud focus                                                   |
| **[OpenStack](https://www.openstack.org/)**                                                                            | OpenStack Foundation | OSS        | De-facto standard for open private clouds                                                  |

<!-- .element: style="font-size: 0.67em;" -->

---
## IaaS: OpenStack

[OpenStack](https://www.openstack.org/): Free and open-source software platform
- Popular choice for private clouds
- Project of Rackspace &amp; NASA (2010, 2016: OpenStack Foundation)
- More than 500 companies have joined the project

Comprised of many sub-projects

| Name     | Functionality | Description                                                                                                                                                                                                                                                                                                                                   |
| -------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Keystone | Identity      | Central user directory and authorization                                                                                                                                                                                                                                                                                                      |
| Nova     | Compute       | Manage compute resources using hypervisors (such as KVM, VMware, ...)                                                                                                                                                                                                                                                                         |
| Neutron  | networking    | Manages networks and IP addresses                                                                                                                                                                                                                                                                                                             |
| Glance   | image service | Manage disk and (cloud) server images                                                                                                                                                                                                                                                                                                         |
| Cinder   | block storage | [Block storage devices](https://en.wikipedia.org/wiki/Block_(data_storage)) for compute instances (using, e.g., [iSCSI](https://en.wikipedia.org/wiki/ISCSI), [Ceph](https://en.wikipedia.org/wiki/Ceph_(software)), [GlusterFS](https://en.wikipedia.org/wiki/GlusterFS), and [others](https://wiki.openstack.org/wiki/CinderSupportMatrix)) |
| Swift    | object store  | scalable redundant storage system for objects (e.g., images stored by Glance)                                                                                                                                                                                                                                                                 |
---
# IaaS and Application Deployment
<!-- .slide: data-name="Application Deployment" -->

---
## Exercise: OpenStack: Create VM

Goal: Run an application in a VM provided by OpenStack

<img src='img/openstack-exercise-application.svg' style='width: 90%'>

Required steps
- Log in to [DHbwCloud](https://dhbw.cloud/) (DHBW VPN required)
- Create a VM, install Linux OS, and connect it to the network,
- Log in (user: `pfisterer-cloud-lecture`), install required software, upload the application and start it

---
## Exercise: OpenStack: Virtual Machine

Prerequisite: Public Key
- Copy your public key to the clipboard (`cat .ssh/id_rsa.pub`) 
- Add it to OpenStack (`Access & Security` | `Key Pairs`) and choose a sensible name

Create a virtual machine 
- Menu item `Compute | Instances | Launch Instance`
- Name: choose a unique name
- Source: choose the newest available Ubuntu image
- Flavor: `mb1.small`
- Network: `DHBW`
- Key Pair: the name assigned to your key pair

---
## Exercise: Solution

<video controls defer data-autoplay style="width: 100%; "> <!-- loop -->
  <source src="img/openstack-create-network-and-vm.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>

---
## Exercise: Install Software (1/2)

Connect to the VM using SSH and install software
- Use `ssh ubuntu@your-vm-ip` 
- It should not require a password

Install [node.js and npm](https://nodejs.org/)
- Run `sudo apt update` to get   available software and their versions
- Run `sudo apt install -y nodejs npm` to install both tools

Create a new Node.js application
- Create a directory (`mkdir app ; cd app`)
- Initialize a [new app](https://docs.npmjs.com/cli/init) (`npm init`) 
- Add [Express.js](https://expressjs.com/en/starter/installing.html) as dependency using `npm i express`

---
## Exercise: Install Software (2/2)

Implement and simple web server
- Create a new file (`server.js`) (cf. [docs](https://expressjs.com/en/starter/hello-world.html))
- Run the server: `node server.js` and verify it is available
- Open `http://your-floating-ip:the-port-of-your-app` (e.g., `http://141.72.177.22:8080`)

Start the web server as a [daemon](https://en.wikipedia.org/wiki/Daemon_(computing))
- Become `root` (`sudo bash`) create the file `/etc/systemd/system/myapp.service` with [this contents](code/examples/systemd-myapp.service)
- Run `systemctl daemon-reload` to reload systemd's configuration
- Run `systemctl enable myapp` and `systemctl start myapp` to enable and (re-)start the app

---
## Summary and Outlook
<!-- .slide: data-name="Summary" -->

IaaS provides on-demand access to virtualized infrastructure
- Virtual machines, storage, and networking as API-driven resources
- Available as public cloud <comment>(hyperscalers)</comment> or private cloud <comment>(HCI)</comment>

We deployed an application manually onto a virtual machine
- SSH into VM, install dependencies, configure/start the application
- This works but does not scale and is hard to reproduce
- Mostly manual steps, error-prone, and not automated
- Prone to "configuration drift" <comment>(over time, manual changes lead to divergence from the original configuration)</comment>

Better to automate the process and make it reproducible
- Requires tools for configuration management and orchestration
