# Releasing

Releases are cut by the `Release` GitHub Actions workflow. Nothing is ever
pushed to `main`: the organisation ruleset makes `main` pull-request only, and
the workflow is built around that.

## Cutting a release

1. Make sure `main` is green and the root `pom.xml` is at the version you want
   to release, with a `-SNAPSHOT` suffix (for example `0.24-SNAPSHOT` releases
   `v0.24`). Override the version in the workflow inputs only for a hotfix
   branch or an unusual jump.
2. Open **Actions → Release → Run workflow** on `main`.
3. Wait for the run to finish, roughly 15 minutes. The workflow:
   - sets the release version in all POMs, commits `Release vX.Y` on a
     detached HEAD and tags it `vX.Y`, all locally;
   - runs `mvn package` with the unit tests on that tagged tree, so the
     distribution is stamped with the tagged commit and
     `git checkout vX.Y && mvn package` rebuilds it from the same commit;
   - pushes only the tag. The tagged commit is one commit off `main` and holds
     the release version in the POMs;
   - creates a **draft** GitHub release with the `-bin.zip` attached and
     auto-generated notes;
   - opens a `chore: bump version to X.(Y+1)-SNAPSHOT` pull request against `main`.
4. Edit the draft release notes and publish the release.
5. Review and merge the bump pull request. It goes through normal CI and
   review like any other change.
6. Publish the connector on Confluent Hub as before; that step is manual and
   outside this workflow.

If the run fails before the tag is pushed nothing has happened and the run can
simply be retried. If the tag was pushed but the draft release could not be
created, or the run was cancelled or timed out at that point, the workflow deletes the draft release, if one was created at all, and
then the tag, so the run can be retried.

## One-time setup

The tag push is the only step that needs a bypass of the organisation
`restrict-tag-pushing` ruleset. That bypass is granted to the organisation's
Maven release GitHub App, the same one `java-questdb-client` uses. The built-in
`GITHUB_TOKEN` cannot push tags, and pull requests it opens do not trigger CI,
so the workflow uses the app token for the tag, the release and the bump PR.

- Install the app on this repository with **Contents: read/write** and
  **Pull requests: read/write**.
- Repository variable `MAVEN_RELEASE_GITHUB_APP_CLIENT_ID`: the app's client ID.
- Environment `maven-release`, secret `MAVEN_RELEASE_GITHUB_APP_PRIVATE_KEY`:
  the app's private key. Adding required reviewers to the environment gives you
  a manual approval gate, but be aware that two jobs use the environment, so
  GitHub asks for approval **twice**: once before the build and the tag push,
  and once more at the very end, before the bump pull request is opened. The
  second prompt arrives after the tag and the draft release already exist, so
  the run looks stuck on a release that has in fact succeeded. If that second
  approval expires, no bump pull request is opened and the next release stops
  at the "tag already exists" guard; open the bump pull request by hand in that
  case.

Never add a bypass for `main`; the bump goes through a pull request on purpose.
