from airflow import settings
from airflow.models import Connection


class ConnectionCreator:
    def create_connection(conn_id, **kwargs):
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if str(conn_name) != str(conn_id):
            # TODO edit connection if it exists
            conn = Connection(
                conn_id=conn_id,
                conn_type='postgres',
                **kwargs
                # host=db_host,
                # schema=self.schema,
                # login=self.login,
                # password=self.password,
                # port=self.port
            )

            session.add(conn)
            session.commit()

        session.close()