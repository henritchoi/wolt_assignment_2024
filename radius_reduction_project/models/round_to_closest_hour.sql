CREATE OR REPLACE FUNCTION round_to_closest_hour(ts TIMESTAMP)
RETURNS TIMESTAMP
STABLE
LANGUAGE SQL
AS $$
  SELECT 
	case when date_trunc('hour', $1) = date_trunc('hour', $1 + interval '30 minutes') then date_trunc('hour', $1 )
  	else date_trunc('hour', $1) + interval '1 hour'
  END;
$$;