embargo-butler
==============

This package provides the services needed to receive notifications of
delivery of images from the Camera Control System to an object store bucket
and automatically ingest them into an appropriate Butler repository.

Containers
----------

Four containers are built from this repo: enqueue, ingest, idle, and presence.

Building
--------

Pull requests will build versions of all four containers tagged with the PR's "head ref".
If a tag (which should always be on main and prefixed with "v") is pushed, versions of all four containers with that tag's version number will be built.
Otherwise, merges to main will result in versions of all four containers tagged with "latest".

If the code in this repo has not changed but a new version of the Science Pipelines stack is needed for the ingest container, the ["On-demand ingest build" workflow](https://github.com/lsst-dm/embargo-butler/actions/workflows/build-manually.yaml) should be executed.
It takes a ref — which should be the latest tag, not usually a branch — a rubin-env version, and the Science Pipelines release tag (e.g. `w_2023_41`).
The tag for the resulting ingest container will contain both the version from the tag of this repo plus the Science Pipelines release tag (e.g. `0.99.2-w_2023_41`).
