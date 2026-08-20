{#
    source() override — defer for sources (dbt-fabricspark)
    ========================================================

    Fabric edition. Same ref()-identical decision rule as the DuckDB variant,
    extended for Fabric's 4-part cross-workspace naming:
    `workspace`.`lakehouse`.`schema`.identifier.

    Assumes the platform guarantees Studio provides:
      1. Every target that runs under `--defer` sets `workspace_name`
         (microsoft/dbt-fabricspark#230), so the relation_names in the manifests
         it reads are 4-part names. Not every target does — CI's compile-only
         target sets none — but those run without `--defer`, where this macro is
         a passthrough.
      2. Lakehouses are schema-enabled — Studio creates them with
         `creationPayload.enableSchemas = true` — so include_policy.database is
         true and the workspace prefix renders. A lakehouse without schemas fails
         at parse: the adapter refuses `workspace_name` against one.
    There is deliberately no 3-part fallback; a violated guarantee fails loudly,
    enforced by the segment-count assert below rather than assumed.

    HOW THE WORKSPACE IS RECOVERED — and why it is the only way
    The relation's `database` is the LAKEHOUSE (credentials.py sets
    `self.database = self.lakehouse`), and Studio names the ephemeral Lakehouse
    after the Domain's, so `database` is identical on both sides and cannot
    discriminate. A profile-level `workspace_name` never reaches node config
    either. What does carry the workspace is `defer_relation.relation_name`, the
    rendered 4-part name — so this macro parses it back out.

    dbt's --defer only rewrites ref(), never source() (dbt-core#10912). This
    ports that defer branch to sources, deriving everything from the Jinja
    context: nothing to pass, nothing to configure.

    BEHAVIOUR (decision rule identical to ref()-defer):
        no --defer                      -> stock dbt, pure no-op
        --defer, table exists locally  -> local relation wins
        --defer, table missing locally -> deferred workspace + lakehouse
        --defer --favor-state          -> deferred workspace + lakehouse always

    REMOVE when https://github.com/dbt-labs/dbt-core/issues/10912 lands.
    Provenance: https://docs.getdbt.com/reference/dbt-jinja-functions/builtins
    Caveat: dbt docs generate bypasses source() overrides
    (https://github.com/dbt-labs/dbt-core/issues/6308).
#}

{% macro source(source_name, table_name) %}

    {% set rel = builtins.source(source_name, table_name) %}

    {% if execute and invocation_args_dict.get('defer') %}

        {# every defer_relation is 4-part: segs = [workspace, lakehouse, schema].
           The identifier is never quoted (quote_policy.identifier is false) while
           database and schema always are, so a 4-part name yields exactly 3
           segments and a 3-part one yields 2 — the count IS the shape check, and
           it must be asserted: on a 3-part name the indexes below would read
           (lakehouse, schema) and silently emit a wrong location rather than
           raising. This is where the no-3-part-fallback guarantee is enforced. #}
        {% set locs = [] %}
        {% for node in graph.nodes.values() %}
            {% set dr = node.get('defer_relation') %}
            {% if dr and dr.get('relation_name') %}
                {% set segs = modules.re.findall('`([^`]*)`', dr['relation_name']) %}
                {% if segs | length != 3 %}
                    {% do exceptions.raise_compiler_error(
                        'source_defer: expected a 4-part defer_relation '
                        ~ '(`workspace`.`lakehouse`.`schema`.identifier), got '
                        ~ (segs | length) ~ ' quoted segments in: '
                        ~ dr['relation_name']
                        ~ ' — the deferred target must set workspace_name against a '
                        ~ 'schema-enabled lakehouse.') %}
                {% endif %}
                {% do locs.append((segs[0], segs[1])) %}
            {% endif %}
        {% endfor %}

        {% if locs %}
            {% set counts = {} %}
            {% for l in locs %}{% do counts.update({l: counts.get(l, 0) + 1}) %}{% endfor %}
            {% set deferred = (counts | dictsort(by='value') | last)[0] %}

            {# same decision rule as ref()-defer #}
            {% if invocation_args_dict.get('favor_state')
                  or not adapter.get_relation(rel.database, rel.schema, rel.identifier) %}
                {% set rel = rel.replace_path(database=deferred[1]).incorporate(workspace=deferred[0]) %}
            {% endif %}
        {% endif %}

    {% endif %}

    {% do return(rel) %}

{% endmacro %}
