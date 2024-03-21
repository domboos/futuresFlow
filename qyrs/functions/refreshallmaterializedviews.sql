CREATE OR REPLACE FUNCTION dbo.refreshallmaterializedviews(
	schema_arg text DEFAULT 'dbo'::text)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
		r RECORD;
	BEGIN
		RAISE NOTICE 'Refreshing materialized view in schema %', schema_arg;
		FOR r IN SELECT matviewname FROM pg_matviews WHERE schemaname = schema_arg
		LOOP
			RAISE NOTICE 'Refreshing %.%', schema_arg, r.matviewname;
			EXECUTE 'REFRESH MATERIALIZED VIEW ' || schema_arg || '.' || r.matviewname;
		END LOOP;

		RETURN 1;
	END
$BODY$;

ALTER FUNCTION dbo.refreshallmaterializedviews(text)
    OWNER TO bood;

GRANT EXECUTE ON FUNCTION dbo.refreshallmaterializedviews(text) TO PUBLIC;