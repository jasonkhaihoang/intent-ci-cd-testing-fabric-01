{#
    source() override — zero-config defer for sources (dbt-duckdb)
    ===============================================================

    dbt's --defer only rewrites ref(), never source(): Manifest.merge_from_artifact
    attaches defer_relation to REFABLE node types only, and sources are skipped
    (dbt-core#10912). This macro gives source() the same semantics, deriving
    everything from what dbt already loads into the Jinja context under --defer.
    Nothing to pass: no env var, no --vars, no wrapper script.

    WHAT IT REUSES FROM THE JINJA CONTEXT
      * graph.nodes[*].defer_relation  -> the merged state-manifest locations
      * adapter.get_relation           -> the same existence check ref() uses
      * invocation_args_dict           -> live --defer / --favor-state flags
        (the `flags` jinja var exposes only global flags; per-command flags live
        in invocation_args_dict)
      * execute                        -> graph/adapter are populated only at
        execution, so at parse time this macro is a pure passthrough

    WHY A CATALOG NAME IS ENOUGH HERE
    The Domain database is attached read-only under its own name, so its catalog
    is the same name the prod target reports and the same name every prod manifest
    records. That only holds because the sandbox file is named after the Intent
    (ephm_<domain>_<intent>.duckdb) rather than after the Domain's file — equal
    stems would make both open as one catalog, and dbt-duckdb refuses a profile
    whose `database` disagrees with its `path`, so the stem cannot be overridden.

    THE ONE INFERENCE: the state manifest's `sources` section is dropped by
    merge_from_artifact before Jinja starts, so the deferred database is inferred
    as the majority database across all merged defer_relations — it assumes
    deferred sources live in the same catalog as deferred models, which is one
    catalog per environment here.

    BEHAVIOUR (decision rule identical to ref()-defer, dbt-core
    context/providers.py RuntimeRefResolver.create_relation):
        no --defer                      -> stock dbt, pure no-op
        --defer, table exists locally  -> local relation wins
        --defer, table missing locally -> deferred database
        --defer --favor-state          -> deferred database always

    REMOVE THIS FILE when either lands in dbt-core:
      * https://github.com/dbt-labs/dbt-core/issues/10912  (allow defer of sources)
      * https://github.com/dbt-labs/dbt-core/issues/9395   (project-level source database)

    Pattern provenance (community-standard builtins override):
      * https://docs.getdbt.com/reference/dbt-jinja-functions/builtins
      * https://github.com/dbt-labs/dbt-core/issues/6308  (also documents the one
        caveat: dbt docs generate catalog queries bypass source() overrides)
      * https://discourse.getdbt.com/t/create-custom-ref-source-macro/431
#}

{% macro source(source_name, table_name) %}

    {% set rel = builtins.source(source_name, table_name) %}

    {% if execute and invocation_args_dict.get('defer') %}

        {# collect the state-manifest databases dbt merged into the graph #}
        {% set defer_dbs = graph.nodes.values()
              | map(attribute='defer_relation') | select
              | map(attribute='database') | select | list %}

        {% if defer_dbs %}

            {# majority database across defer_relations = the deferred catalog #}
            {% set counts = {} %}
            {% for d in defer_dbs %}{% do counts.update({d: counts.get(d, 0) + 1}) %}{% endfor %}
            {% set deferred_db = (counts | dictsort(by='value') | last)[0] %}

            {# same decision rule as RuntimeRefResolver.create_relation #}
            {% if invocation_args_dict.get('favor_state')
                  or not adapter.get_relation(rel.database, rel.schema, rel.identifier) %}
                {% set rel = rel.replace_path(database=deferred_db) %}
            {% endif %}

        {% endif %}

    {% endif %}

    {% do return(rel) %}

{% endmacro %}
