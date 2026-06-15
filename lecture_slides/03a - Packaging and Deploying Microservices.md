<div class="lecturetitle">Packaging and Deploying Microservices</div>
<!-- .slide: data-name="Introduction" data-state="hide-menubar" -->

---
## Packaging Micro-Services

Microservices are packaged and deployed independently
- Packaged individually <comment>(with all dependencies)</comment>
- Isolated from each other at run-time
- Interact over a network

Different packaging and deployment options exist
- Example: use Virtual Machine Images (e.g., using Packer)
- Creates machine images from a single source configuration
- Provides hardware-level isolation between different VMs

Virtualization on hardware level is resource-intensive
- Each VM includes a full operating system and its own kernel
- Alternative: User-Space Isolation

---
## Isolation on Operating System Level

Until now: isolation on hardware level
- Applications run inside virtualized hard- and software environment

<img src='img/virtualization-3-many-apps-on-virtualized-os.svg' style='width: 95%; padding-left: 20px;'>

Now: run application inside virtualized software environment only
- Make applications believe they are the only one
- Requires operating system support
- Linux provides user-space isolation <comment>(control groups, namespaces, ...)</comment>

---
## Linux: User-Space Isolation
<!-- .slide: data-name="User-Space Isolation" -->


<img src='img/linux-container-app-isolation.svg' style='width: 100%;'>

---
## Linux: User-Space Isolation

<img src='img/docker-kernel-libraries.svg' style='width: 100%;'>

---
## Linux + Docker: Application Containers

Linux: user-space isolation
- Run isolated applications on the same operating system
- Applications share the kernel but run in custom environment
- Environment: code, run-time, tools, config, libraries, OS files, ...

Docker: application deployment with user-space isolation
- Package applications + environment as container images
- Consequence: applications all look the same <comment>(from the outside)</comment>

<img src='img/linux-container-app-packaging.svg' style='width: 98%; padding-left: 20px;'>

---
## Container Workflow

<img src='img/linux-container-app-distribution.svg' style='width: 100%;'>

Workflow
- Developers provide generic application blueprint <comment>(Dockerfile)</comment>
- Creates container image 
- Pushes container to registry <comment>([Docker Hub](https://hub.docker.com/) or private hosted)</comment>
- Compute instances pull required images from registry
- Start containers <comment>(i.e., instances)</comment> from container images

Standardized development, packaging, distribution, and deployment of applications

---
## Docker: Building Container Images
<!-- .slide: data-name="Building Containers" -->

Build container images from application code and dependencies
- Established 2015 by Docker, now hosted by the Linux Foundation
- Format defined by the [Open Container Initiative](https://opencontainers.org/) (OCI)

Creating a container image
- Lego-like way to construct container images (i.e., step-by-step)
- Textual description of the steps required to build such an image
- Dockerfile: text file with instructions to build a container image

Dockerfile
- Select parent image <comment>(e.g., [Scratch](https://hub.docker.com/_/scratch), [Ubuntu Linux](https://hub.docker.com/_/ubuntu), [Node.js](https://hub.docker.com/_/node), etc.)</comment>
- Add application code and dependencies <comment>(software, tools, libraries, ...)</comment>
- Define startup command to be executed when the container starts

---
## Building Containers Images: Dockerfile

Example: containerizing a Java application
- Comprised of a single [JAR](https://en.wikipedia.org/wiki/JAR_(file_format)) file created by a developer
- Requires a Java Runtime Environment ([JRE](https://en.wikipedia.org/wiki/Java_virtual_machine#Execution_environment))

Dockerfile
- Reference JRE image (e.g., [OpenJDK](https://hub.docker.com/_/openjdk) with tag `8-jre-alpine`)
- Add JAR file and define startup command
  
<img src='img/docker-dockerfile-and-layers.svg' style='width: 95%'>

---
## Docker: Build Container Image

<!-- 
  
  Run: 
    docker builder prune -a
    ~/docker-delete-images.sh
    asciinema rec --overwrite -i 2 img/docker-build-demo.cast 
    rm -rf docker-build-demo
-->
<asciinema data-conf='{ "cols": 140, "rows": 30, "theme":"monokai", "autoPlay": true, "idleTimeLimit": 2, "terminalFontSize": "13px"}'
        src="img/docker-build-demo.cast" />

Sources: [Jar-File](code/dockerfile-java/my-java-app-0.0.1.jar), [Dockerfile](code/dockerfile-java/Dockerfile), for commands navigate &darr;

vvv
## Docker: Build Container Image

<a data-code='bash' data-link href='code/examples/dockerfile-java/create-docker-image.sh'>Source code</a>

---
## Layered Filessystem: Shared Layers

Similar container image share layers
- Space-efficient storage <comment>(automatic de-deduplication)</comment>

<img src='img/docker-shared-layers.svg' style='width: 95%'>


---
## Automatic Deduplication with UnionFS

<img src='img/docker-union-file-system.svg' style='width: 72%; padding-left: 20px;'>

Originally developed for [Live CD](https://en.wikipedia.org/wiki/Live_CD) environments
- Lower layers are read-only <comment>(e.g., using a CD or DVD medium)</comment>
- Top layer is read-write <comment>(e.g., using a [file system in RAM](https://en.wikipedia.org/wiki/RAM_drive))</comment>
- Uses [copy-on-write](https://en.wikipedia.org/wiki/Copy-on-write) to allow editing existing files


---
## Dockerfile: Creating Layers

Most commands create new layers
- Add contents to a container image: `ADD` and `COPY`
- Run a command during container creation: `RUN`

[ADD](https://docs.docker.com/engine/reference/builder/#add) and [COPY](https://docs.docker.com/engine/reference/builder/#copy)
- `COPY [--chown=<user>:<group>] <src>... <dest>`
- `ADD [--chown=<user>:<group>] <src>... <dest>`
- `src` is relative to the directory containing the `Dockerfile`
- If `dest` does not exist, it is created
- ADD is more powerful than COPY (can [accept URLs, etc.](https://stackoverflow.com/questions/24958140/what-is-the-difference-between-the-copy-and-add-commands-in-a-dockerfile))

Example using `COPY`
- `COPY index.js package.json /src`

---
## Dockerfile: Creating Layers

Running commands: [RUN](https://docs.docker.com/engine/reference/builder/#run)
- `RUN <command>`
- `RUN ["executable", "param1", "param2"]`

Both execute a command during container creation
- Runs command in a new layer on top of the current image
- Commits the results to the new layer
- New image is be used for the next step in the Dockerfile

Examples
- Install debian packages: `RUN apt update && apt-get install -y nodejs python3` 
- Install Python packages: `RUN pip3 install numpy`

---
## Dockerfile: Docker Build Cache

Building an image
- `docker build` steps through the instructions in a Dockerfile
- Each instruction is executed in the specified order
- Can be a very time-consuming <comment>(especially during development)</comment>

To speed up image building, Docker uses a build cache
- Checks for each instruction if this step has been executed before
- It true, it can reuse the created layer <comment>(rather than creating a new one)</comment>

Cache invalidation
- If a step deviates from a previously cached sequence of commands, the build cache is not used anymore
- Some commands always invalidate the cache

---
## Dockerfile: Docker Build Cache

Caching with `ADD` and `COPY`
- Cache lookup: checksum compared against those in existing images
- On changes <comment>(contents or metadata)</comment>: cache is invalidated
- Subsequent steps do not use the cache

`RUN` and the build cache
- Docker compares only the raw command string
- If the same command was executed before, the cached is used
- `RUN apt-get update` will only be run once <comment>(and keep old versions)</comment>

Avoid using the build cache
- Build without cache: `docker build --no-cache=true`
- Delete build cache:  `docker builder prune -a -f`

---
## Dockerfile: Startup Command

Starting a container instance
- Effectively starts a new process from a container image
- Container runtime needs to know which command to run

Dockerfiles: `ENTRYPOINT` and `CMD`
- `ENTRYPOINT` (defaults to `/bin/sh -c`)
- `CMD` (no default value)
- cf. [Understand how CMD and ENTRYPOINT interact](https://docs.docker.com/engine/reference/builder/#understand-how-cmd-and-entrypoint-interact)

Frequently, `CMD` is used to customize the startup command
- `CMD ["/bin/bash"]`
- `CMD ["/usr/bin/java", "-jar", "/app.jar"]`

---
## Docker: Starting Container Instances
<!-- .slide: data-name="Running Containers" -->

Starting a container instance
- `docker run [OPTIONS] IMAGE [COMMAND] [ARG...]`
- Creates top-layer where the instance may write on top of the images' layers

Example: run command `ls -la` in Ubuntu Linux 
- `docker run --rm ubuntu:latest ls -la`
- Start instance from image `ubuntu` with tag `latest` <comment>(default)</comment>
- If `[COMMAND]` is not present, the one in `Dockerfile` is used

After the command has completed, the instance exits 
- The container's top layer is removed (because of `--rm`)
- Otherwise the layer would remain intact (check `docker ps -a` to see exited instances)

---
## Docker: Starting Containers

Container name
- Add `--name` to assign a unique name to the container
- Otherwise, a random name is assigned

Interactive container instances
- `-t`: allocate a terminal
- `-i`: allocate standard input (stdin)
- Shortcut: `-ti` 
- For additional details cf. [Confused about Docker -t option to Allocate a pseudo-TTY](https://stackoverflow.com/questions/30137135confused-about-docker-t-option-to-allocate-a-pseudo-tty))

---
## User-Space Isolation in Containers

Run a standard container instance
- Run `docker run --rm  -ti debian bash` twice
- Install psmisc: `apt-get update && apt-get install -y psmisc`
- Run a sleep command in the background: `sleep 1000 &`
- Run `pstree`

Run a privileged container instance on the host
- `docker run -it --rm --privileged --pid=host debian bash`
- Install `psmisc` and run `pstree`

---
## Docker: Starting &amp; Stopping Containers

Use `-d` (i.e., detached) to run the container in the background
- Run `docker run -d  -p 8080:8080 --name my-container-name my-app `
- Inspect that it is running using `docker ps`
- Mostly used for servers and daemonized workloads

Stop and remove containers
- `docker rm -f my-container-name`

---
## Docker: Volumes
<!-- .slide: data-name="Volumes" -->

Servers need to store persistent data (e.g., databases)
- [Volumes](https://docs.docker.com/engine/reference/builder/#volume) survive container (re-)starts
- Can be declared in Dockerfiles (e.g., `VOLUME /data`)

Makes a host path available in a container instance
- So-called [mounting](https://en.wikipedia.org/wiki/Mount_(Unix)) maps a host path to container path on startup

Docker: two ways of [mounting a volume](https://docs.docker.com/storage/volumes/)
- `--volume` / `-v`: `docker run -d -v /some/absolute/folder/on/the/host:/data my-app`
- `--mount`: `docker run -d --mount source=/some/absolute/path/on/the/host,target=/app my-app`

---
## Docker: Networking
<!-- .slide: data-name="Networking" -->

Docker offers [several networking types](https://docs.docker.com/network/)  
- Examples: bridge, host, overlay, macvlan, none, and others
- So-called *Container Network Interface (CNI)*

[Bridge](https://docs.docker.com/network/bridge/) is the default one

<img src="img/docker-networking.png" style="width: 67%; padding-left: 20px;">

---
## Docker: Bridge Networking

Each container has its own IP address
- This is source [NATed](https://en.wikipedia.org/wiki/Network_address_translation)
- Consequence: Container *sees* a different IP that others

A container's ports must be reverse NATed to be useful
- Dockerfile should [expose](https://docs.docker.com/engine/reference/builder/#expose) a container's ports (e.g., `EXPOSE 80`)
- A container instance must [publish](https://docs.docker.com/engine/reference/commandline/run/#publish-or-expose-port--p---expose) internal to external ports 
- Map host port to container port: `-p hostport:containerport`

Example
- E.g., publish container port `80` on host port `8080`
- `docker run --rm -ti -p 8080:80 --name my-container-name my-app`

---
## Docker: Exercise
<!-- .slide: data-name="Exercise" -->

Containerize your node.js application
- Use this [node base image](https://hub.docker.com/_/node/) for your [FROM](https://docs.docker.com/engine/reference/builder/#from) instruction
- Add your application (or use this [index.js](code/docker-basic/index.js) and [package.json](code/docker-basic/package.json)) to `/src`
- Install dependencies (change to `/src` using [WORKDIR](https://docs.docker.com/engine/reference/builder/#workdir) and [RUN](https://docs.docker.com/engine/reference/builder/#run) `npm install`)
- [Expose](https://docs.docker.com/engine/reference/builder/#expose) your application's port (e.g., 8080)
- Set the [CMD](https://docs.docker.com/engine/reference/builder/#cmd) to execute when the container starts
- [Run](https://docs.docker.com/engine/reference/run/) an instance of your application

Further reading
- [Best practices for writing Dockerfiles](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)

---
## Microservices: Docker: Solution

<a data-code='Dockerfile' data-link href="code/docker-basic/Dockerfile">Source code</a>

---
## Docker Multi-CPU-Architecture Support

Many different [combinations of CPU architectures and OSs](https://github.com/docker-library/official-images#architectures-other-than-amd64) exist
- E.g., Linux x86-64 (`amd64`) vs. Windows x86-64 (`windows-amd64`)
- Others: Linux on IBM POWER8, IBM z Systems, RISC-V 64-bit, ...
- Requires different [instruction sets](https://en.wikipedia.org/wiki/Instruction_set_architecture) and [ABIs](https://en.wikipedia.org/wiki/Application_binary_interface)

Non-x86 CPU architectures are gaining popularity
- Especially [ARM](https://en.wikipedia.org/wiki/ARM_architecture_family) (Advanced RISC Machines)
- Raspberry Pi 1 Model A: `arm32v6`
- Apple M1: `arm64v8`

Docker images can/should support multiple architectures
- Requires building [multiple images](https://docs.docker.com/desktop/multi-arch/) for these architectures

---
## Docker Buildx

Docker Desktop can run cross-architecture (Linux) containers
- E.g., to run `amd64` containers on Apple M1 chips (`arm64v8`)
- Uses emulation with [QEMU](https://www.qemu.org/) (slow) 

Use [Docker Buildx](https://docs.docker.com/buildx/working-with-buildx/) to build cross-architecture container images
- Create a builder: `docker buildx create --name mybuilder`
- Use a builder: `docker buildx use mybuilder`

Build (and push) a multi-arch container image

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64,linux/arm/v7 \
  -t yourusername/yourcontainername:1.2.3 \
  -t yourusername/yourcontainername:latest \
  --push \
  .
```

---
## Alternatives to Docker

Building container images
- [moby/buildkit](https://github.com/moby/buildkit)
- [GoogleContainerTools/kaniko](https://github.com/GoogleContainerTools/kaniko)

Container runtimes
- [containerd](https://containerd.io/)
- [CRI-O](https://cri-o.io/)
- [runc](https://github.com/opencontainers/runc)
- [gVisor](https://github.com/google/gvisor)
- [Nabla Containers](https://nabla-containers.github.io/)
