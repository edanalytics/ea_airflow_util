from airflow.models import Connection
from airflow import settings

def create_conn(conn_id, login, password, conn_type='http', host=None):
    
    # TODO add more connection param options
    
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        login=login,
        host=host
    )
    new_conn.set_password(password)
    
    session = settings.Session()
    session.add(new_conn)
    session.commit()

def list_conn(pattern='%'): # note, '%' = any char in sqlalchemy
    
    # TODO add more filter options
    
    session = settings.Session()
    connections = session.query(Connection).filter(
        Connection.conn_id.contains(pattern)
    )
    
    conn_ids = list(map(lambda conn: conn.conn_id, connections))
    
    return(conn_ids)

def update_conn(conn_id, conn_type=None, description=None, host=None, login=None, password=None, schema=None, port=None, extra=None, uri=None):
    
    session = settings.Session()
    try:
        # Get connection object
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).one()
    except exc.NoResultFound:
        conn = None
    except exc.MultipleResultsFound:
        conn = None
    
    if conn:
        # Update connection
        conn.conn_type = conn_type or conn.conn_type
        conn.description = description or conn.description
        conn.host = host or conn.host
        conn.login = login or conn.login
        conn.password = password or conn.password
        conn.schema = schema or conn.schema
        conn.port = port or conn.port
        conn.extra = extra or conn.extra
        conn.uri = uri or conn.uri
        session.add(conn)
        session.commit()

def delete_conn(conn_id):
    
    session = settings.Session()
    try:
        # Get connection object
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).one()
    except exc.NoResultFound:
        conn = None
    except exc.MultipleResultsFound:
        conn = None
    
    if conn:
        # Delete connection object
        session.delete(conn)
        session.commit()
