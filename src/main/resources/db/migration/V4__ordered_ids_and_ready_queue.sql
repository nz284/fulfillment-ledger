-- UUIDv7 layout for PostgreSQL 17. Existing UUIDs are opaque and remain valid.
-- Keep the random variant bits and 74 random bits from gen_random_uuid().
CREATE FUNCTION ledger_uuid_v7() RETURNS uuid
LANGUAGE sql VOLATILE PARALLEL SAFE AS $$
    SELECT encode(
        substring(int8send(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
        || substring(set_byte(random_bytes, 6, (get_byte(random_bytes, 6) & 15) | 112) FROM 7),
        'hex')::uuid
    FROM (SELECT uuid_send(gen_random_uuid()) AS random_bytes) AS seed;
$$;

CREATE INDEX work_ready_ordered ON work_item(kind,available_at,id) WHERE NOT done;
DROP INDEX work_ready;
