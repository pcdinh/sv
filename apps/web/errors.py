from vibora import Route
from vibora import Request, Response, MethodNotAllowed, NotFound
from start_app import app


@app.handle(MethodNotAllowed)
async def internal_server_error(request: Request):
    return Response(
        b'Method Not Allowed', status_code=405,
        headers={'Allow': request.context['allowed_methods']}
    )


@app.handle(NotFound)
async def not_found(request: Request):
    return Response(
        b'URL not found', status_code=404
    )
