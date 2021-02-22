from airflow.providers.postgres.hooks.postgres import PostgresHook
import contextlib

@contextlib.contextmanager
def postgres(conn_id):
    postgres = PostgresHook(postgres_conn_id=conn_id)
    conn = postgres.get_conn()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()