from sqlalchemy import create_engine, text


DATABASE_URL = "postgresql://postgres:Olalekan1996*@localhost:5433/ecocast_db"

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:

    db = conn.execute(
        text("SELECT current_database();")
    ).fetchone()

    print("Database:", db[0])

    tables = conn.execute(
        text("""
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname='public';
        """)
    )

    for row in tables:
        print(row)