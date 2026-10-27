import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: flink_stream.py <window_type> (tumbling / sliding / session)")
        sys.exit(1)

    window_type = sys.argv[1]

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("table.exec.source.idle-timeout", "1000 ms")

    create_kafka_source_ddl = """
        CREATE TABLE kafka_events (
            sensor_id STRING,
            temp DOUBLE,
            `timestamp` STRING,
            ts AS TO_TIMESTAMP(REPLACE(`timestamp`, 'Z', '')),
            WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'events',
            'properties.bootstrap.servers' = 'kafka-lab2:29092',
            'properties.group.id' = 'flink_group_unique_1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(create_kafka_source_ddl)

    if window_type == "tumbling":
        sql_query = """
            SELECT 
                TUMBLE_START(ts, INTERVAL '10' SECOND) as win_start,
                TUMBLE_END(ts, INTERVAL '10' SECOND) as win_end,
                sensor_id,
                COUNT(*) as event_count
            FROM kafka_events
            GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), sensor_id
        """
    elif window_type == "sliding":
        sql_query = """
            SELECT 
                HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as win_start,
                HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as win_end,
                sensor_id,
                COUNT(*) as event_count
            FROM kafka_events
            GROUP BY HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), sensor_id
        """
    elif window_type == "session":
        sql_query = """
            SELECT 
                SESSION_START(ts, INTERVAL '10' SECOND) as win_start,
                SESSION_END(ts, INTERVAL '10' SECOND) as win_end,
                sensor_id,
                COUNT(*) as event_count
            FROM kafka_events
            GROUP BY SESSION(ts, INTERVAL '10' SECOND), sensor_id
        """
    else:
        print("Unknown window type! Choose: tumbling, sliding, session")
        sys.exit(1)

    print(f"Starting Flink job with {window_type} window...")
    table = t_env.sql_query(sql_query)
    table.execute().print()