# elementary_fabricspark (vendored)

Fabric Spark adapter macros for Elementary — dispatch overrides that let Elementary compile on `dbt-fabricspark`. This directory is a **vendored copy**; the canonical source is <https://github.com/accelerate-data/vd-dbt-elementary-fabricspark> (vendored from commit `c5b0af7740dee272b43bacd81cff36ae3cc9567c`).

Why vendored: the Fabric dbt job runtime rejects every git-URL dbt package ("Git-based dbt packages are not supported"), so `packages.yml` references this copy as `- local: ./vendored/elementary_fabricspark` instead of the git pin. Hub packages are unaffected.

**Updating:** the upstream repo stays canonical. To re-vendor after an upstream change, copy `macros/fabricspark_overrides.sql` verbatim from upstream, merge any `dbt_project.yml` change while keeping the Studio header comment it carries here, and record the new upstream commit in this file — an explicit maintenance action, never automatic.
