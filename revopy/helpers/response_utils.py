# -*- coding: utf-8 -*-

import rapidjson
from vibora.responses import Response


class JsonResponse(Response):

    def __init__(self, content: object, status_code: int = 200, headers: dict = None, cookies: list = None):
        self.status_code = status_code
        mode = rapidjson.DM_ISO8601 | rapidjson.DM_NAIVE_IS_UTC
        self.content = rapidjson.dumps(content, datetime_mode=mode).encode()
        self.headers = headers or {}
        self.headers['Content-Type'] = 'application/json'
        self.cookies = cookies or []


class WebResponse(Response):

    def __init__(self, content: object, status_code: int = 200, headers: dict = None, cookies: list = None):
        self.status_code = status_code
        self.content = content.encode("utf-8")
        self.headers = headers or {}
        self.cookies = cookies or []
