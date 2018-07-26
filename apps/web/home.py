from start_app import app, managed, Config
from vibora import Request, Response
from vibora import Route
from revopy.helpers.response_utils import JsonResponse, WebResponse


@app.route('/')
async def home(request: Request, config: Config):
    config = request.app.components.get(Config)

    return JsonResponse(
               {
                   'hello': 'world',
                   'config': config.POSTGRESQL_DSN,
                   'pg_pool': hasattr(request.app, "pg")
               }
           )


@app.route('/home')
async def home(request: Request):
    try:
        request.app.components.get(Route)
        return Response(b'Second')
    except Exception as error:
        return Response(str(error).encode(), status_code=500)


@app.route('/c')
async def test_connection(request: Request):
    try:
        # Take a connection from the pool.
        ''':type : asyncpg.connection.Connection'''
        async with request.app.pool.acquire() as connection:
            # Open a transaction.
            async with connection.transaction():
                # Run the query
                result = await connection.fetchval('SELECT 2 ^ $1', 2)
                config = request.app.components.get(Config)
                return WebResponse('Result {}. DEBUG: {}'.format(result, config.DEBUG))
    except Exception as error:
        return WebResponse(str(error), status_code=500)


@app.route('/c1', methods=[b"GET", b"POST"])
async def test_connection(request: Request):
    try:
        import datetime
        # Take a connection from the pool.
        ''':type : asyncpg.connection.Connection'''
        async with request.app.pg.pool.acquire() as connection:
            # Open a transaction.
            async with connection.transaction():
                # Run the query
                await connection.execute(
                          '''INSERT INTO users(user_id, first_name, last_name, source, status, created_time) 
                             VALUES($1, $2, $3, $4, $5, $6)''',
                          1, "Lionen", "Messi", 1, 1, datetime.datetime.utcnow()
                      )
                result_01 = await connection.fetchrow('SELECT user_id, first_name FROM users')
                result_02 = await connection.fetchrow('SELECT user_id, first_name FROM users WHERE user_id = 10')
                result_03 = await connection.fetch('SELECT user_id, first_name FROM users WHERE user_id = 10')
                await connection.execute('''DELETE FROM users WHERE user_id = $1''', 1)
                config = request.app.components.get(Config)
                return WebResponse('Result {}. Keys: {}, Values: {}, Empty: {}, DEBUG: {}'.format(result_01, result_01.keys(), result_01.values(), result_03, config.DEBUG))
    except Exception as error:
        return WebResponse(str(error), status_code=500)


@app.route('/c2', methods=[b"GET", b"POST"])
async def test_connection(request: Request):
    import traceback
    import sys
    try:
        import datetime
        async with managed(request.app.pg) as connection:
            ''':type : revopy.ds.postgresql.SessionManager'''
            rs1 = await connection.fetch_one("SELECT user_id, first_name FROM users")
            rs2 = await connection.fetch_one(
                            "SELECT user_id, first_name FROM users WHERE user_id = %(user_id)s",
                            {"user_id": 1}
                        )
            rs3 = await connection.fetch_value(
                            "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                            {"user_id": 1}
                        )
            rs4 = await connection.fetch_column(
                            "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                            {"user_id": 1}
                        )
            rs5 = await connection.fetch_all(
                            "SELECT user_id FROM users WHERE user_id = %(user_id)s",
                            {"user_id": 1}
                        )
            return JsonResponse(
                       {
                           "rs1": rs1,
                           "rs2": rs2,
                           "rs3": str(rs3),
                           "rs4": rs4,
                           "rs5": rs5
                       }
                   )
    except Exception as error:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tbe = traceback.TracebackException(
            exc_type, exc_value, exc_tb,
        )
        e1 = ''.join(tbe.format())
        e2 = ''.join(tbe.format_exception_only())
        return WebResponse(
                   str(error) + ":" + type(error).__name__ + ">> \n" + str(e1) + "\n\n" + str(e2), status_code=500
               )
