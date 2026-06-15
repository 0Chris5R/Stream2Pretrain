<div class="lecturetitle">Introduction</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Slides Available Online

<a class="urlforslides" style="font-size: 200%"></a>

<canvas style="width: 50%"class="qrcodeforslides"></canvas>

---
## Software

> Software is eating the world.
> <br><br>
> Marc Andreessen, 2000
<!-- .element: style="font-size: 2.5em;" -->

---
## Software

> **Software is also eating much of the value chain of industries that are widely viewed as primarily existing in the physical world.**
> 
> In today's cars, software runs the engines, controls safety features, entertains passengers, guides drivers to destinations and connects each car to mobile, satellite and GPS networks...
> 
> The trend toward hybrid and electric vehicles will only accelerate the software shift–electric cars are completely computer controlled. And the creation of software-powered driverless cars is already under way at Google and the major car companies.
>
> Marc Andreessen: "Why Software Is Eating The World", Wall Street Journal, 2011

<!-- .element: style="font-size: .99em;" -->

---
## Networking

> The **CPU as an island**, contained and valuable in itself, **is dying in the nineties**.
> The **next paradigm of computing is distributed** or cooperative computing. 
> 
> This is driven by the very real demands of corporations **recognizing information as an asset**, perhaps their most important asset.
> 
> To **make use of information** effectively, it must be accurate and **accessible across** the department, even across **the world**. 
> 
> This means that CPUs must be intimately linked to the networks of the world and be capable of freely passing and receiving information, not hidden behind glass and cooling ducts or the complexities of the software that drives them.
>
> Object Management Group: "Object Management Architecture Guide". Revision 2.0, September 1992

<!-- .element: style="font-size: 0.95em;" -->

---
## Entstehung von Daten-Produkten
<!-- .slide: data-background-image="img/piero-scaruffi.png" data-background-size="100%" -->

<div style="text-align: left; font-size: 1.3em; font-weight: bold; line-height: 175%;">
The difference between oil and data is that the product of oil does not generate more oil (unfortunately), whereas the product of data (self-driving cars, drones, wearables, etc) will generate more data (where do you normally drive, how fast/well you drive, who is with you, etc).
</div>

<p style="text-align: right;">
	<a href="http://www.scaruffi.com/politics/sv.html">Piero Scaruffi, 2016</a>
</p>

<credits>
	<a href="https://papelcult.files.wordpress.com/2016/06/scaruffi.jpg">Background Image Source</a>
</credits>

---
## Data Products

Generate data through use and improve themselves from it
- Google Search <comment>(advertising via AdWords)</comment>
- Google Maps <comment>(traffic information)</comment>
- Stackoverflow <comment>(ranking better answers)</comment>
- Facebook <comment>(personalization, advertising)</comment>
- Netflix <comment>(personalization, recommendations)</comment>
- Amazon <comment>(personalization, recommendations, reviews)</comment>

Products are conceived globally from day one
- Potentially exponential growth in user numbers
- Traditional IT cannot keep up with this growth
- Consequence: Cloud Computing and Big Data

---
## Essential Characteristics (cf. [NIST](https://doi.org/10.6028%2FNIST.SP.800-145))
<!-- .slide: data-name="Cloud Properties" style="font-size: 0.94em;" -->

1\. On-demand self-service
- Consumers provision computing capabilities automatically 
- No human interaction on the provider side

2\. Broad network access
- Capabilities are available over the network <comment>(Internet)</comment>

3\. Resource pooling
- Provider's resources are pooled (multi-tenant model)
- Can be dynamically (re-)assigned according to demand

4\. Rapid elasticity
- (Semi-) Automatically provision/release to scale with demand

5\. Measured service
- Resource monitored, controlled, and reported

---
## Cloud: Deployment Models

Public cloud
- Publicly available; owned by an organization selling cloud services

Private cloud
- Operated solely for an organization <comment>(on/off premise)</comment>
- Managed by the organization or a third party

Community cloud
- Shared by a specific community <comment>(e.g., police, fireservice, country)</comment>
- Managed (on/off premise) by the community of a third party

Hybrid cloud
- Composition of two or more clouds <comment>(private, community, or public)</comment>
- Combined to allow data and application portability <comment>(e.g., for load-balancing between clouds)</comment>

---
## Cloud: Service Models

<img src='img/cloud-service-models.svg' style='width: 92%'>

---
## Cloud: Software as a Service (SaaS)

Definition

> The capability provided to the **consumer is to use the provider's applications running on a cloud infrastructure**. 
> 
> The applications are accessible from various client devices through either a thin client interface, such as a web browser (e.g., web-based email), or a program interface. 
> 
> The **consumer does not manage or control** the underlying cloud infrastructure including network, servers, operating systems, storage, or even **individual application capabilities**, with the possible exception of limited user-specific application configuration settings.
> 
> ([NIST](https://doi.org/10.6028%2FNIST.SP.800-145))

Audience: end users
- Examples: Google Docs, Dropbox, Salesforce, Cisco WebEx, ...

---
## Cloud: Platform as a Service (PaaS)

Definition

> The capability provided to the **consumer is to deploy** onto the cloud infrastructure **consumer-created or acquired applications** created using programming languages, libraries, services, and tools supported by the provider. 
> 
> The **consumer does not manage or control the underlying cloud infrastructure** including network, servers, operating systems, or storage, **but has control over the deployed applications** and possibly configuration settings for the application-hosting environment.
> 
> ([NIST](https://doi.org/10.6028%2FNIST.SP.800-145))

Audience: application developers
- OS, middleware, and runtime provided
- Consumers don't "see" the underlying infrastructure

---
## Cloud: Platform as a Service (PaaS)

Use case: Node.js application
- Developer
  - Provides application and `package.json` (dependencies)
  - Configures requirements: [Node.js](https://nodejs.org/en/) and a [MariaDB](https://mariadb.org/) database
  - Consumer pushes application code to a remote [git](https://git-scm.com/) repository
- PaaS provides required infrastructure
  - Compute <comment>(CPU, RAM)</comment>
  - Storage <comment>(virtual hard disk)</comment>
  - Networking <comment>(DNS names, IP address, network)</comment>
  - Platform <comment>(e.g., Node.js runtime, NPM dependencies, and Database)</comment>

Examples
- Commercial: [AWS Elastic Beanstalk](https://aws.amazon.com/elasticbeanstalk/), [Google App Engine](https://cloud.google.com/appengine/), [Red Hat OpenShift PaaS](https://www.redhat.com/en/explore/PaaS), [Heroku](https://www.heroku.com/)
- Open Source: [Dokku](https://github.com/dokku/dokku), [Deis](https://deis.com/), [Tsuru](https://tsuru.io/), [CapRover](https://caprover.com/)

---
## FaaS / Serverless Computing

Variant of PaaS (same audience: application developers)
- Defines triggers (e.g., HTTP GET) and functions (`handle_get`)
- Provider takes care of invoking the functions for certain triggers

<img src='img/cloud-serverless.svg' style='width: 90%'>

---
## FaaS / Serverless Computing

"Serverless" is a misleading term
- Cloud provider runs servers and dynamically manages allocation of resources to functions
- Pricing based on amount of resources consumed by an application

Commercial offerings
- AWS [Lambda](https://aws.amazon.com/lambda/)
- [Google Cloud Functions](https://cloud.google.com/functions)

Open Source projects
- [Apache OpenWhisk](https://openwhisk.apache.org/)
- [Fission](https://fission.io/)
- [Kubeless](https://kubeless.io/)
- [OpenFaaS](https://docs.openfaas.com/)

---
## Infrastructure as a Service (IaaS)

Definition

> The capability provided to the consumer is to provision **processing, storage, networks**, and other fundamental computing resources where the consumer is able to deploy and run arbitrary software, which can include operating systems and applications. 
>
> [...] where the **consumer is able to deploy and run arbitrary software**, which can include operating systems and applications. The **consumer does not manage or control the underlying cloud infrastructure** but **has control over operating systems, storage, and deployed applications**; and possibly limited control of select networking components (e.g., host firewalls). ([NIST](https://doi.org/10.6028%2FNIST.SP.800-145))


Audience: infrastructure architects
- Provides basic infrastructure building blocks (mostly in software)

---
## Beyond IaaS: Platform Engineering

IaaS: powerful but complex
- Developers must understand networking, VMs, storage, and security
- PaaS abstracts this, but commercial offerings are often too restrictive or create vendor lock-in

Platform Engineering: building Internal Developer Platforms (IDP)
- Self-service layer on top of IaaS / Kubernetes
- Operated by a dedicated platform team

Provides "golden paths"
- Opinionated, pre-approved workflows for building, deploying, and running services
- Developers consume infrastructure via abstractions (forms, APIs, templates)

---
## Cloud-Native Applications
<!-- .slide: data-name="Cloud Native Apps" -->

> Cloud native technologies empower organizations to **build and run scalable applications in modern, dynamic environments such as** public, private, and hybrid **clouds**. 
> 
> **Containers**, **service meshes**, **microservices**, **immutable infrastructure**, and declarative APIs exemplify this approach.
> 
> These techniques enable **loosely coupled systems** that are **resilient, manageable, and observable**. 
> 
> Combined with **robust automation**, they allow engineers to **make** high-impact **changes frequently** and predictably with minimal toil.
> 
> The Cloud Native Computing Foundation seeks to drive adoption of this paradigm by fostering and sustaining an ecosystem of open source, vendor-neutral projects. We democratize state-of-the-art patterns to make these innovations accessible for everyone.
> 
> Source: Technical Oversight Committee (TOC), CNCF Foundation
<!-- .element: style="font-size: 0.9em;" -->


---
## Cloud-Native Applications

Cloud-native applications...
- are designed to operate in cloud environments
- leverage cloud infrastructure and services 
- maximize scalability, flexibility, resilience, and efficiency

Microservices Architecture
- Small, independent services
- Communicate over well-defined APIs

Containerization (e.g., Docker)
- Packaging of applications (and dependencies) into isolated containers
- Portable and consistent across different environments

---
## Cloud-Native Applications

Orchestration and Management
- E.g., Kubernetes to manage containers at scale 
- Provides automation, scaling, network management, and troubleshooting
- Ensure that the right containers run on the right resources at the right time

Dynamic Scalability
- I.e., elastic scalability
  
DevOps and CI/CD 
- Continuous Integration and Continuous Deployment 
- For rapid and continuous delivery of changes

---
## Cloud-Native Applications

Infrastructure Independence
- Independent of underlying infrastructure (e.g., AWS, Azure, Google Cloud)
- Avoids vendor lock-in
- Run in different, hybrid, or multi-cloud environments

Automation and Infrastructure as Code
- Infrastructure and configurations defined and versioned in code
- Ensuring consistency and repeatability

---
## Cloud and Digital Sovereignty

Digital sovereignty
- Ability of individuals, organizations, and states to control their own digital infrastructure, data, and processes

Understanding cloud infrastructure allows organizations to
- Make informed build-vs-buy decisions <comment>(which services to operate vs. consume from a provider)</comment>
- Assess and reduce vendor lock-in <comment>(APIs, data formats, egress costs)</comment>
- Enforce data residency and compliance requirements <comment>(GDPR, NIS2, sectoral regulations)</comment>
- Migrate workloads between providers or back on-premises

Without this understanding, technical decisions are delegated to vendors

---
## Cloud and Digital Sovereignty
<!-- .slide: data-name="Cloud and Digital Sovereignty" -->

Political and regulatory context
- EU GAIA-X initiative: federated, interoperable European cloud infrastructure
- European Chips Act, EU Cloud Rulebook: reducing strategic dependency on non-European infrastructure
- Public sector increasingly requires certified, sovereign cloud environments <comment>(e.g., BSI C5, EUCS)</comment>

Open source as an enabler
- Open standards and open-source software allow organizations to avoid single-vendor dependencies
- Kubernetes, OpenStack, and Crossplane are examples of vendor-neutral building blocks

---
## Agenda
<!-- .slide: data-stack-name="Agenda" -->

<div data-toc-src="generated_toc.html" style="font-size: 0.85em; margin-top: -1em;" />

vvv
## Ideas for Topics
<!-- .slide: style="font-size: 0.7em;" -->

Operator-Based Management of [insert some tool here] Installations
  - Automate the deployment, scaling, and management of Kubeflow on Kubernetes using custom operators
  - E.g., Binderhub-Installations in a separate namespace per user. Use Network policies to create isolated environments.

Scalable E-commerce Platform
- Build a microservices-based e-commerce application that can scale horizontally
	Real-time Chat Application:
-  Design a chat application with microservices for user management, messaging, notifications, and real-time updates using WebSockets and WebPush, hosted on a Kubernetes cluster. 

Serverless Event-Driven Application
- Develop a serverless application using a FaaS tool on e.g., Kubernetes, focusing on event-driven architecture and horizontal scalability.

Multi-Cloud Deployment Manager
- Build a tool that automates creates a Kubernetes cluster spanning multiple-cloud providers
	IoT Data Processing Pipeline
- Design a scalable pipeline for ingesting, processing, and visualizing IoT data using microservices, Kubernetes, and Helm, with operators for dynamic scaling based on data load.

vvv
## Ideas for Topics
<!-- .slide: style="font-size: 0.7em;" -->

AI-Powered Customer Support Chatbot
-  Create a chatbot service powered by AI, with microservices for different chatbot functionalities, managed with Kubernetes and deployed using Helm for version control and updates
- Run multiple instances of an open source LLM and scale depending on the load

Personalized Recommendation Engine
- Build a recommendation engine for e-commerce or media platforms, using microservices for data processing and machine learning, deployed on Kubernetes with Helm for configuration management.

Microservices-Based Travel Booking System
- Design a travel booking system with separate services for flights, hotels, and car rentals, managed on Kubernetes and Helm, ensuring horizontal scalability and fault tolerance.

Self-Hosted LLM Chatbot
- Deploy an open-source large language model on Kubernetes, with a microservices architecture for handling requests, scaling, and monitoring.
- Run multiple instances of an open source LLM and scale depending on the load

vvv
## Ideas for Topics
<!-- .slide: style="font-size: 0.7em;" -->

Document Summarization Service
- Create a service for summarizing lengthy documents using an open-source LLM, deployed on Kubernetes.

Retrieval-Augmented Generation (RAG) for Knowledge Bases
- Develop a RAG system that combines LLMs with a retrieval system to answer questions based on a custom knowledge base, using Kubernetes for deployment and management.

Language Translation Service
- Deploy an open-source LLM-based translation model on Kubernetes, with microservices for handling translation requests, scaling, and logging.
- Integrate with a messenger service (e.g., Telegram)

AI-Powered Code Generation Tool
- Create a code generation tool using an open-source LLM fine-tuned for programming languages, deployed on Kubernetes, with Helm for configuration and updates.

Personalized Content Recommendation System
- Build a recommendation system that uses an open-source LLM to generate personalized content suggestions, with a microservices architecture for scalability and reliability.

vvv
## Ideas for Topics
<!-- .slide: style="font-size: 0.7em;" -->

Document Analysis and Generation
- Develop a service for analyzing and generating documents using an open-source LLM, with Kubernetes for handling deployment, scaling, and updates.

Educational Tutoring System
- Create an AI-powered tutoring system using an open-source LLM to provide explanations and answer questions in various subjects, deployed on Kubernetes with Helm for seamless updates.

Customer Support Automation with LLMs
- Implement a customer support automation platform using an open-source LLM for generating responses to customer queries, managed with Kubernetes for high availability and scalability.

News Aggregation and Summarization Service:
- Develop a news aggregation service that uses an open-source LLM to summarize and categorize news articles, deployed on Kubernetes with Helm for easy updates.

Cloud Development Platform
-	Implement a scalable developer platform in the cloud using only a browser to access code.
-	Use e.g., https://code.visualstudio.com/docs/editor/vscode-web and deploy a pod per user.

---
## Required Knowledge

Networking
- [HTTP](https://developer.mozilla.org/en-US/docs/Web/HTTP), [RESTful APIs](https://developer.mozilla.org/en-US/docs/Glossary/REST)

Unix / Linux
- Command line (bash, ssh)
- Networking (ifconfig, ping, dig, curl, ...)

Programming
- JavaScript (Language basics, Node.js, NPM)
- Python (Language basics)

Misc
- JSON and YAML

vvv
## Using the Command Line

Provides a command line user interface
- Interface for humans to interact with the operating system
- Both an interactive command language and a scripting language
- Also called _Shell_

Windows
- [cmd.exe](https://en.wikipedia.org/wiki/Cmd.exe)
- [PowerShell](https://docs.microsoft.com/en-us/powershell/scripting/getting-started/getting-started-with-windows-powershell?view=powershell-6)

Unix / Linux shell
- Bourne again shell `bash` (this lecture)
- Others: Bourne shell `sh`, Thompson shell `osh`, C shell `csh`, Korn shell `ksh`, Z-Shell `zsh`, ...

vvv
## Linux Bash

Bourne Again Shell (<a href="https://en.wikipedia.org/wiki/Bash_(Unix_shell)">bash</a>)
- Popular shell on Linux

Get a Bash environment
- Install on Windows (e.g., [Cygwin](https://www.cygwin.com/) or [WSL2](https://en.wikipedia.org/wiki/Windows_Subsystem_for_Linux))
- Install a virtual machine (using [VirtualBox](https://www.virtualbox.org/wiki/Downloads) and [Ubuntu](https://www.osboxes.org/ubuntu/))

Follow the following tutorials
- [Basic linux command line tutorial](https://www.techspot.com/guides/835-linux-command-line-basics/)
- [Introduction to Bash](https://astrobiomike.github.io/bash/index)
- [Ryans Tutorials - Linux Tutorial](https://ryanstutorials.net/linuxtutorial/)

vvv
## Unix / Linux Bash

Concepts
- [Unix / Linux architecture](https://en.wikipedia.org/wiki/Unix_architecture)
- [Everything is a file](https://en.wikipedia.org/wiki/Everything_is_a_file)
- [Unix file system](https://en.wikipedia.org/wiki/Unix_filesystem)
- [Users and access rights](https://www.digitalocean.com/community/tutorials/an-introduction-to-linux-permissions)
- [Input / Output redirection](https://www.guru99.com/linux-redirection.html)
- [Environment Variables](https://codeburst.io/linux-environment-variables-53cea0245dc9)

---
## Password-less SSH Access

Prepare SSH on your computer
- Create private/public key pair (run `ssh-keygen`, cf. [this doc](https://help.github.com/articles/connecting-to-github-with-ssh/))
- Additional reading: [Connecting to GitHub with SSH](https://help.github.com/en/articles/connecting-to-github-with-ssh)
