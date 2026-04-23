{% snapshot mutable_zones_snapshot %}

{{ config(
    target_schema=target.schema ~ '_snapshots',
    unique_key='location_id',
    strategy='check',
    check_cols=['borough', 'zone', 'service_zone']
) }}

select * from {{ ref('mutable_zones') }}

{% endsnapshot %}
