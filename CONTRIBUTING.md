# Contribution Guidelines

Thanks for your interest in contributing to the ZDM Proxy!

Below you'll find some guidelines to help you get started.
There are multiple ways you can contribute to the project, you can open Pull Requests
to fix a bug or add a new feature, report issues or improve the documentation.

The proxy is written in Go, so if you're already familiar with the language, you'll notice
the project uses most of the standard tools and conventions in the Go ecosystem for code formatting, building
and testing. But if you're still new to it, don't worry, we'll give you an overview in the next
few sections.

- [Code Contributions](#code-contributions)
  - [Running Unit Tests](#running-unit-tests)
  - [Running Integration Tests](#running-integration-tests)
    - [Simulacron](#simulacron)
    - [CCM](#ccm)
  - [Debugging](#debugging)
  - [Pull Request Checks](#pull-request-checks)

## Code Contributions

---
**Note:** If you're considering to submit a patch to the project, make sure you agree with the
Contribution License Agreement (CLA) available at https://cla.datastax.com/. You'll be required
to sign it with your GitHub account before we can accept your code changes.
---

Before you start working on a feature or major change, we ask you to discuss your idea with the members
of the community by opening an issue in GitHub, this way we can discuss design considerations and
feasibility before you invest your time on the effort.

When  you're ready to code, follow these steps:

1. If you haven't done so yet, go ahead now and fork the ZDM Proxy repository into your own Github account.
That's how you'll be able to make contributions, you cannot push directly to the main repository, but
you can open a Pull Request for your branch when you're ready.


2. From your fork create a new local branch where you'll make your changes. Give it a short, descriptive, name.
If there's an associated issue open, include it as a prefix, *zdm-#issue-name*, for example: zdm-10-fix-bug-xyz.


3. Make the changes you want locally and validate them with the [test tools](#running-unit-tests) provided. Push the
updated code to the remote branch in your fork.


4. Finally, when you're happy with your patch, open a Pull Request through the GitHub UI.


5. The automated tests in GitHub Actions will kick in and in a few minutes you should
have some results. If any of the checks fail, take a look at the [logs](#pull-request-checks) to understand what happened.
You can push more commits to the PR until the problem is solved, the checks will run again.


6. Once all checks are passing, your PR is ready for review and the members of the ZDM Proxy community will
start the process. You may be asked to make further adjustments, this is an iterative process, but
in the end if everything looks good, you'll be able to merge the PR to the main branch.


7. Congratulations! At this point your code is ready to be released in the official distribution in the
next release cycle. Keep an eye on the Releases page for new versions: https://github.com/datastax/zdm-proxy/releases

### Running Unit Tests

The main source code of the project is located under the [proxy](https://github.com/datastax/zdm-proxy/tree/main/proxy)
folder, but you'll notice some files have the following name pattern `*_test.go` next to the main file,
these are the unit tests. These tests are meant to run very quickly with no external dependencies.

To run the unit tests, go the root of the project in a terminal window and execute the following command:

> $ go test -v ./proxy/... 

You should see a PASS or FAIL message at the end of the execution.

Make sure you add tests to your PR if you're making a major contribution.

### Running Integration Tests

The integration tests have different execution modes that allow you to test the proxy with
an in-memory CQL server, a mock service ([Simulacron](https://github.com/datastax/simulacron)), or an actual database
(managed by [CCM](https://github.com/riptano/ccm)).
By the default, it will run both the in-memory and Simulacron tests, but you can selectively
enable the ones you want with the command-line flags we'll see next.

#### Simulacron

Simulacron is a native protocol server simulator for Apache Cassandra&reg; written in Java. It allows us to test the
protocol message exchanges over a socket from the proxy to the backend without having to run a fully-fledged database.

It needs to be installed separately, and for that follow the installation instructions available
[here](https://github.com/datastax/simulacron#prerequisites).

Now set the `SIMULACRON_PATH` environment variable to the path of the jar file you downloaded in the previous step.

Simulacron relies on loopback aliases to simulate multiple nodes. On Linux or Windows, you shouldn't have anything to do.
On MacOS, run this script:

```bash
#!/bin/bash
for sub in {0..4}; do
    echo "Opening for 127.0.$sub"
    for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
done
```
It may take a couple of minutes for it to complete and settle down. Also, note that this does not survive reboots.

Now you should be ready to run the Simulacron tests with:

> $ go test -v ./integration-tests

#### CCM

Cassandra Cluster Manager (CCM) is a tool written in Python that manages local Cassandra installations for testing purposes.
These tests will be run automatically by Github when you open a PR, but you can optionally run them locally too,
for that follow the installation instructions available in the CCM [repository](https://github.com/riptano/ccm#installation).

Once you have CCM installed, run the tests with:

> $ go test -v ./integration-tests -RUN_CCMTESTS=true

By the default, that will include the mock tests too (CQL Server and Simulacron), but you can disabled them and run CCM
only with:

> $ go test -v ./integration-tests -RUN_CCMTESTS=true -RUN_MOCKTESTS=false

### Running Docker Compose

If you simply want to try things out and play with the proxy locally, in order to do any meaningful interaction with it
you'll need to connect it to two database clusters (ORIGIN and TARGET). To simplify that process we provide a
docker-compose definition that handles the setup for you.

The definition will start two Cassandra clusters, one ZDM proxy instance and then finally run a NoSQLBench workload to
verify the setup.

Start the services with the following command (this assumes you have docker-compose installed):

> $ docker-compose -f docker-compose-tests.yml up

That will keep the containers running in case you want to do any further manual inspection. To stop the services, hit
CTRL+C and delete the containers with:

> $ docker-compose -f docker-compose-tests.yml down

### Debugging



### Pull Request Checks