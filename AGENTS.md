# Agent Instructions for Apache Cassandra

> These instructions apply to all AI-assisted contributions to `apache/cassandra`.

## Apache Cassandra
Apache Cassandra is a NoSQL distributed database. This is the official Git repository

## Environment

- Java 11 (default) or 17.
- Python 3 for `cqlsh` and dtests.
- Apache Ant >= 1.10 for all builds. Do NOT attempt to use Maven, Gradle, or any other build tool. Cassandra uses Ant exclusively.
- Do NOT attempt to install dependencies, you do not have Internet access

## Build

```bash
ant build                # compile all classes (includes Accord submodule)
ant jar                  # build the main JAR
ant clean                # remove locally created artifacts
ant realclean            # remove entire build directory and downloaded artifacts
```

Do NOT run `ant build` if you only need to verify a small change compiles.

## Testing

- Do NOT run the entire test suite(`ant test`). Run only the specific test(s) relevant to your change.

    ```bash
    # Run a single unit test class
    ant testsome -Dtest.name=org.apache.cassandra.service.StorageServiceServerTest -Dtest.methods=testGetAllRangesEmpty
    ```

- When fixing a bug, first create a regression test that reproduces the failure, then implement the fix and verify.
- Provide test(s) coverage for all new or modified code.

## Linting and Code Checks

```bash
ant check                # runs checkstyle, RAT license check, and builds
ant checkstyle           # checkstyle only
```

## Code Style
Cassandra enforces style via Checkstyle (`ant checkstyle`). Key rules are included in `checkstyle.xml` file.
General style:
- 4-space indentation, no tabs.
- Braces on a new line below control statements (Allman style).
- Brace-less style for single-line control statements.
- Match existing code style in the file you are editing.
- All new files must include the Apache License 2.0 header.
- Concise English documentation is required for complex classes and methods; trivial ones may not require them.

## Git Workflow
- Do NOT commit unless explicitly asked.
- Commit messages should reference the JIRA issue. Disclose that AI assistance was used in the PR description.

    ```
    CASSANDRA-XXXXX: Brief description of the change
    Author: Your Name <your.email@example.com>
   
    Co-authored-by: GitHub Copilot
    Co-authored-by: Claude
    Co-authored-by: gemini-code-assist
    ```

- Do NOT modify submodule references without understanding the implications. Submodule changes must be committed and pushed before the parent Cassandra commit.

## Boundaries

- 🚫 Never modify generated files in `src/gen-java/` — they are built from `src/antlr/` grammars.
- 🚫 Never modify files in `lib/` — these are managed dependencies.
- 🚫 Never commit secrets, credentials, or API keys.
- 🚫 Never run the full test suite (`ant test`) — it takes hours. Run targeted tests only.
- 🚫 Never bypass Checkstyle violations without a suppression comment explaining why.
- 🚫 Never create summary or documentation files unless explicitly asked.
- ⚠️ Ask before modifying the CQL grammar (`src/antlr/Cql.g`) — changes cascade widely.
- ⚠️ Ask before modifying `modules/accord/` — it is a separate repository.
