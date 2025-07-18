# Intro

## Commands

***All commands assume you are in the root directory of this project***.
For me, that looks like `~/repos/chronon`.

## Prerequisites

Add the following to your shell run command files e.g. `~/.bashrc`.

```
export CHRONON_OS=<path/to/chronon/repo>
export CHRONON_API=$CHRONON_OS/api/python
alias materialize="PYTHONPATH=$CHRONON_API:$PYTHONPATH $CHRONON_API/ai/chronon/repo/compile.py"
```

### Install latest version of thrift

Thrift is a dependency for compile. The latest version is 0.22 jan 2025.

```shell
brew install thrift
```

### Install Python dependency packages for API

```shell
python3 -m pip install -U tox build
```

### Install appropriate java, scala, and python versions

* Install [asdf](https://asdf-vm.com/guide/getting-started.html#_2-download-asdf)
* ```asdf plugin add asdf-plugin-manager```
* ```asdf install asdf-plugin-manager latest```
* ```asdf exec asdf-plugin-manager add-all``` (see `.plugin-versions` for required plugins)
* ```asdf exec asdf-plugin-manager update-all```
* ```asdf install``` (see `.tool-versions` for required runtimes and versions)

> NOTE: Use scala `2.12.18` and java `corretto-17` for Zipline distribution. older java `corretto-8` is used for OSS
> Chronon distribution.

### Clone the Chronon Repo

```bash
git clone git@github.com:zipline-ai/chronon.git
```

## Bazel Setup

### Installing Bazel

#### On Mac

```shell
# Install bazelisk and it automatically pulls right bazel binary
brew install bazelisk
```

#### On Linux

```shell
sudo curl -L "https://github.com/bazelbuild/bazelisk/releases/download/v1.18.0/bazelisk-linux-amd64" -o /usr/local/bin/bazel
sudo chmod +x /usr/local/bin/bazel
export PATH="/usr/local/bin:${PATH}"
```

### Configuring IntelliJ

- Install `Bazel For IntelliJ` Plugin
- Follow File > Import Bazel Project
    - Select root directory as workspace
    - Use `.bazelproject` as project view file
- We should see a bazel icon in the top right corner to the left of search bar
    - Used for incremental sync after build config changes
    - The first build might take some time, ~15 minutes or so
- We can directly build and test all our targets from IntelliJ

### Remote Caching

We enabled remote caching for all our builds/tests for both local development and CI.
As part of that change we would need to do gcloud auth to read/write from remote cache stored in our BigTable bucket for
the local dev builds.

#### For passing GCloud Auth credentials to Bazel

Create a new .bazelrc.local file with the following content. Also feel free to specify any local overrides to the
build/test options here.
This file is git-ignored.

```
build --google_credentials=/Users/{username}/.config/gcloud/application_default_credentials.json
```

### Pinning maven artifacts

We currently pin the versions for all our maven artifacts including all their transitive dependencies so
we don't have to resolve them during build time which can take up a very long time at times.

We currently have 2 different repositories

1. spark - contains all spark dependencies (pinned to spark_install.json file)
2. maven - contains all other maven dependencies (pinned to maven_install.json file)

Whenever we change any of the dependency artifacts in the above repositories we would need to re-pin and
update the json files using the below commands which need to be checked in

```shell
# For maven repo
REPIN=1 bazel run @maven//:pin
# For spark repo
REPIN=1 bazel run @spark//:pin
```

### Java not found error on Mac

In case you run into this error the fix is to manually download and install amazon corretto-17
from [here](https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/downloads-list.html)

### Build Uber Jars for deployment

```shell
# Command
# By default we build using scala 2.12
bazel build //{module}:{target}_deploy.jar

# Cloud Gcp Jar
# Creates uber jar in {Workspace}/bazel-bin/cloud_gcp folder with name cloud_gcp_lib_deploy.jar
bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar
# Flink Jars
bazel build //flink:flink_assembly_deploy.jar
bazel build //flink:flink_kafka_assembly_deploy.jar

# Service Jar
bazel build //service:service_assembly_deploy.jar

# Hub Jar
bazel build //hub:hub_assembly_deploy.jar
```

> Note: "_deploy.jar" is bazel specific suffix that's needed for building uber jar with all
> transitive dependencies, otherwise `bazel build //{module}:{target}` will only include
> dependencies specified in the target definition

### All tests for a specific module

Also it's lot easier to just run from IntelliJ

```shell
# Example: bazel test //api:tests
bazel test //{module}:{test_target}
```

### Only test individual test file within a module

```shell
# Example: bazel test //api:tests_test_suite_src_test_scala_ai_chronon_api_test_DataPointerTest.scala
bazel test //{module}:{test_target}_test_suite_{test_file_path}
```

### To clean the repository for a fresh build

```shell
# Removes build outputs and action cache.
bazel clean
# This leaves workspace as if Bazel was never run.
# Does additional cleanup compared to above command and should also be generally faster
bazel clean --expunge
```

## Pushing code

We run formatting a auto-fixing for scala code. CI will fail if you don't do this
To simplify your CLI - add the following snippet to your zshrc

```sh
alias bazel_scalafmt='bazel query '\''kind("scala_library.*", //...)'\'' | xargs -I {} bazel run {}.format'

function zpush() {
    if [ $# -eq 0 ]; then
        echo "Error: Please provide a commit message."
        return 1
    fi

    local commit_message="$1"

    bazel_scalafmt && \
    git add -u && \
    git commit -m "$commit_message" && \
    git push

    if [ $? -eq 0 ]; then
        echo "Successfully compiled, formatted, committed, and pushed changes."
    else
        echo "An error occurred during the process."
    fi
}
```

You can invoke this command as below

```
zpush "Your commit message"
```

> Note: The quotes are necessary for multi-word commit message.

## Connect remotely to API Docker JVM

The java process within the container is started with remote
debugging [enabled](https://github.com/zipline-ai/chronon/blob/main/docker-init/start.sh#L46) on port 5005
and [exposed](https://github.com/zipline-ai/chronon/blob/main/docker-init/compose.yaml#L70) on the host as
`localhost:5005`. This helps you debug frontend code by triggering a breakpoint in IntelliJ when some code in the
frontend is run (i.e. api call, etc)

To connect to the process within the container via IntelliJ, follow these steps:

1. Open IntelliJ and go to `Run` > `Edit Configurations`.
2. Click the `+` button to add a new configuration.
3. Select `Remote JVM Debug` from the list.
4. Enter `localhost:5005` as the host and port (defaults)
5. Click `Debug`.
6. Set a breakpoint in the code you want to debug.
7. Run the frontend code that will call the api (or call the API endpoint directly such as with `curl`/Postman/etc).
8. When the breakpoint is hit, you can inspect variables, step through the code, etc.

For more details see IntelliJ remote
debugging [tutorial](https://www.jetbrains.com/help/idea/tutorial-remote-debug.html)

## Old SBT Setup

### Configuring IntelliJ

- Open the project from the root `chronon` directory.
- Under File > Project Structure > Platform Settings, add java `corretto-17` and scala `scala-2.12.18` SDKs.
- Under Intellij IDEA > Settings > Editor > Code Style > Scala enable `scalafmt`.
- Follow the steps below to configure unit tests in intellij:

  Run > Edit Configurations
  <!-- ![](./intellij_unit_test_1.png) -->

  Set the
  following [java arguments](https://stackoverflow.com/questions/72724816/running-unit-tests-with-spark-3-3-0-on-java-17-fails-with-illegalaccesserror-cl)
  by copy pasting into the run configuration arguments list:
  ```bash
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  ```
  <!-- ![](./intellij_unit_test_2.png) -->

  Then, set the classpath to `chronon/<module_name>`
  <!-- ![](./intellij_unit_test_3.png) -->
- Do the same for `ScalaTests` as well.
- Run
  an [example test](https://github.com/zipline-ai/chronon/blob/main/spark/src/test/scala/ai/chronon/spark/test/bootstrap/LogBootstrapTest.scala)
  in Chronon to verify that you’ve set things up correctly.

  From CLI: `sbt "testOnly ai.chronon.spark.test.TableUtilsFormatTest"`

**Troubleshooting**

Try the following if you are seeing flaky issues in IntelliJ

```
sbt +clean
sbt +assembly
```

### Generate python thrift definitions

```shell
sbt py_thrift
```

### Materializing confs

```
materialize  --input_path=<path/to/conf>
```

### Testing

All tests

```shell
sbt test
```

Specific submodule tests

```shell
sbt "testOnly *<Module>"
# example to test FetcherTest with 9G memory
sbt -mem 9000 "test:testOnly *FetcherTest"
# example to test specific test method from GroupByTest
sbt "test:testOnly *GroupByTest -- -t *testSnapshotEntities"
```

### Check module dependencies

```shell
# ai.zipline.overwatch.Graph based view of all the dependencies
sbt dependencyBrowseGraph

# Tree based view of all the dependencies
sbt dependencyBrowseTree
```

# Chronon Build Process

* Inside the `$CHRONON_OS` directory.

## Using sbt

### To build all of the Chronon artifacts locally (builds all the JARs, and Python API)

```shell
sbt package
```

### Build Python API

```shell
sbt python_api
```

Note: This will create the artifacts with the version specific naming specified under `version.sbt`

```text
Builds on main branch will result in:
<artifact-name>-<version>.jar
[JARs]   chronon_2.11-0.7.0-SNAPSHOT.jar
[Python] chronon-ai-0.7.0-SNAPSHOT.tar.gz


Builds on user branches will result in:
<artifact-name>-<branch-name>-<version>.jar
[JARs]   chronon_2.11-jdoe--branch-0.7.0-SNAPSHOT.jar
[Python] chronon-ai-jdoe--branch-ai-0.7.0-SNAPSHOT.tar.gz
```

### Build a fat jar

```shell
sbt assembly
```

### Building a fat jar for just one submodule

```shell
sbt 'spark/assembly'
```

# Chronon Artifacts Publish Process

* Inside the `$CHRONON_OS` directory.

To publish all the Chronon artifacts of the current git HEAD (builds and publishes all the JARs)

```shell
sbt publish
```

* All the SNAPSHOT ones are published to the maven repository as specified by the env variable `$CHRONON_SNAPSHOT_REPO`.
* All the final artifacts are published to the MavenCentral (via Sonatype)

NOTE: Python API package will also be generated, but it will not be pushed to any PyPi repository. Only `release` will
push the Python artifacts to the public repository.

## Setup for publishing artifacts to the JFrog artifactory

1. Login into JFrog artifactory webapp console and create an API Key under user profile section.
2. In `~/.sbt/1.0/jfrog.sbt` add

```scala
credentials += Credentials(Path.userHome / ".sbt" / "jfrog_credentials")
```

4. In `~/.sbt/jfrog_credentials` add

```
realm=Artifactory Realm
host=<Artifactory domain of $CHRONON_SNAPSHOT_REPO>
user=<your username>
password=<API Key>
```

## Setup for publishing artifacts to MavenCentral (via sonatype)

1. Get maintainer access to Maven Central on Sonatype
    1. Create a sonatype account if you don't have one.
        1. Sign up here https://issues.sonatype.org/
    2. Ask a current Chronon maintainer to add you to Sonatype project.
        1. To add a new member, an existing Chronon maintainer will need
           to [email Sonatype central support](https://central.sonatype.org/faq/what-happened-to-issues-sonatype-org/#where-did-issuessonatypeorg-go)
           and request a new member to be added as a maintainer. Include the username for the newly created Sonatype
           account in the email.
2. `brew install gpg` on your mac
3. In `~/.sbt/1.0/sonatype.sbt` add

```scala
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
```

4. In `~/.sbt/sonatype_credentials` add

```
realm=Sonatype Nexus Repository Manager
host=s01.oss.sonatype.org
user=<your username>
password=<your password>
```

5. setup gpg - just first step in
   this [link](https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html#step+1%3A+PGP+Signatures)

## Setup for pushing python API package to PyPi repository

1. Setup your pypi public account and contact @Nikhil to get added to the PyPi package as
   a [collaborator](https://pypi.org/manage/project/chronon-ai/collaboration/)
2. Install `tox, build, twine`. There are three python requirements for the python build process.

* tox: Module for testing. To run the tests run tox in the main project directory.
* build: Module for building. To build run `python -m build` in the main project directory
* twine: Module for publishing. To upload a distribution run `twine upload dist/<distribution>.whl`

```
python3 -m pip install -U tox build twine
```

3. Fetch the user token from the PyPi website.
4. Make sure you have the credentials configuration for the python repositories you manage. Normally in `~/.pypirc`

```
[distutils]
  index-servers =
    local
    pypi
    chronon-pypi

[local]
  repository = # local artifactory
  username = # local username
  password = # token or password

[pypi]
  username = # username or __token__
  password = # password or token

# Or if using a project specific token
[chronon-pypi]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = # Project specific pypi token.
```

# Chronon Release Process

## Publishing all the artifacts of Chronon

1. Run release command (e.g. `gh release create v0.0.xx`) in the right HEAD of chronon repository, or on the UI, click 
 `Releases` button on the right side of the repository and then select "Draft a new release". When creating a new 
release, make sure to tag the release with the next version number.

This command will take into the account the name of the tag and handles a series of events:

* Rebuilds the wheel based on the last passing integration tests
* Publishes them to GCP and AWS artifacts buckets if the tests pass
* Also publishes the artifacts to jfrog artifactory if the tests pass
* If the tests fail it will convert the release to a draft


# Testing on REPL

`One-time` First install the ammonite REPL with [support](https://ammonite.io/#OlderScalaVersions) for scala 2.12

```shell
sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/com-lihaoyi/Ammonite/releases/download/3.0.0-M0/2.12-3.0.0-M0) > /usr/local/bin/amm && chmod +x /usr/local/bin/amm' && amm
```

Build the chronon jar for scala 2.12

```shell
sbt ++2.12.12 spark/assembly
```

Start the REPL

```shell
/usr/local/bin/amm
```

In the repl prompt load the jar

```scala
import $cp.spark.target.`scala-2.12`.`spark-assembly-0.0.63-SNAPSHOT.jar`
```

Now you can import the chronon classes and use them directly from repl for testing.


# Debugging Unit Tests with Spark SQL

When running Spark unit tests, data is written to a temporary warehouse directory. You can use a Spark SQL shell to inspect this data directly, which is helpful for debugging test failures or understanding what's happening in your tests.

## Finding the Warehouse Directory

First, locate the warehouse directory in your test logs:

1. Look for "warehouse" in your test output:
   ```bash
   # When running a test
   sbt "testOnly my.test.Class" | grep -i warehouse
   
   # Or check the logs in IntelliJ test output window
   ```

2. You should see a log line similar to:
   ```
   Setting default warehouse directory: file:/tmp/chronon/spark-warehouse_f33f00
   ```

3. The path after `file:` is your warehouse directory (e.g., `/tmp/chronon/spark-warehouse_f33f00`)

## Installing and Running Spark SQL Shell

Install Apache Spark using Homebrew (macOS):

```bash
brew install apache-spark
```

Run the Spark SQL shell pointing to your warehouse directory:

```bash
spark-sql --conf spark.sql.warehouse.dir=/tmp/chronon/spark-warehouse_f33f00
```

## Exploring Data in the Spark SQL Shell

Once in the Spark SQL shell, you can explore the data:

```sql
-- List all databases
SHOW DATABASES;

-- Use a specific database
USE your_database_name;

-- List all tables
SHOW TABLES;

-- Query a table
SELECT * FROM your_table LIMIT 10;

-- Check table schema
DESCRIBE your_table;

-- Look at specific partition
SELECT * FROM your_table WHERE ds = '2023-01-01';
```

This approach is useful for various test debugging scenarios, such as:
- Examining actual vs. expected data in failed assertions
- Checking if data was written correctly by your test
- Understanding join and aggregation results
- Verifying partitioning is correct

# Working with the Python API on Pycharm
1. Download Pycharm
2. Open up Pycharm at `chronon/api` directory. Limiting the IDE with just this directory will help the IDE not get confused when resolving imports like `ai.chronon...` as IDE may attempt to go to the `java` or `scala` modules instead. Also helpful tip: `Invalidated Caches / Restart` from the `File` menu can help resolve some of the import issues.
3. Then `Mark Directory as` > `Sources Root` for the `py` directory.

![PyCharm](../../images/dev_pycharm.png)
