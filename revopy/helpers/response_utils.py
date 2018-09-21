# -*- coding: utf-8 -*-

import rapidjson
from vibora.responses import Response


STATUS_OK = 200  # The request has been fulfilled successfully
STATUS_CREATED = 201  # POST: "Created" success code, for POST request.
STATUS_ACCEPTED = 202  # POST: The request has been accepted, but the processing has not been completed (queued)
# http://stackoverflow.com/questions/3050518/what-http-status-response-code-should-i-use-if-the-request-is-missing-a-required
# Un-processable Entity: unsupported parameter (in JSON content or GET params)
STATUS_BAD_REQUEST = 400  # Invalid input
STATUS_ACCESS_DENIED = 401  # Authentication required: missing access token or API Key. User is unrecognized
STATUS_HANDSHAKE_DENIED = 402  # Invalid secret/activation code

# http://stackoverflow.com/questions/3297048/403-forbidden-vs-401-unauthorized-http-responses
STATUS_FORBIDDEN = 403  # Forbidden: Authorization. Authenticated user can not access the resource
STATUS_NOT_FOUND = 404  # The request resource not found. If the API endpoint does not exist
STATUS_INVALID_METHOD = 405  # Wrong HTTP method to an existing end-point

# Conflicted data: same email address used by 2 accounts ...
STATUS_CONFLICT = 409  # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.10
STATUS_INVALID_INPUT = 406
STATUS_NOT_SUPPORT = 410  # The request resource not support

# http://tools.ietf.org/html/rfc4918#section-11.4
# User submits valid parameters: GET or POST but it does not make sense (incorrect password?)
# http://www.bennadel.com/blog/2434-HTTP-Status-Codes-For-Invalid-Data-400-vs-422.htm
STATUS_UNPROCESSABLE = 422
# Your application is sending too many simultaneous requests. http://docs.stormpath.com/rest/product-guide/
STATUS_TOO_MANY_REQUESTS = 429  # too many request
STATUS_INTERNAL_ERROR = 500  # Server code error
STATUS_SERVICE_NOT_IMPLEMENTED = 501

# We are temporarily unable to service the request. Please wait for a bit and try again.
# - subcode: 5031 -> System maintenance
# - subcode: 5032 -> Subsystem overload or unreachable
STATUS_SERVICE_UNAVAILABLE = 503
STATUS_OVER_QUOTA = 507  # https://www.dropbox.com/developers/core/docs

# Response sub-codes
SUBCODE_INCORRECT_CREDENTIALS = 100
SUBCODE_UNQUALIFIED_KEY = 101
SUBCODE_IDENTITY_UNVERIFIED = 102
SUBCODE_PERM_NOT_OWNER = 103
SUBCODE_VALID_IDENTITY_NOT_FOUND = 104  # see STATUS_UNPROCESSABLE - for user
SUBCODE_TAKEN_SECONDARY_EMAIL = 105
SUBCODE_VALID_KEY_NOT_FOUND = 106  # used for activation or sign-up key
# E.x: Request to /user/<user-id>?access_token=<string> where user-id does not match the UID stored with access-token
SUBCODE_RESOURCE_CONFLICT = 107
SUBCODE_VALID_OBJECT_NOT_FOUND = 108  # user/actor -> perform action -> object (can be user or file ID)
SUBCODE_TIME_CONFLICT = 109
SUBCODE_ATTRIBUTE_CONFLICT = 110
SUBCODE_RESOURCE_EXPIRED = 111  # file access expired, user deleted
SUBCODE_PERM_INSUFFICIENT = 112  # no right permission to access the resource

# Those codes are used when user sends requests using invalidated access tokens
SUBCODE_AUTH_KICKED = 113  # session is deleted by unlinking device
SUBCODE_AUTH_PASSWORD_RESET = 114  # session is deleted b/c team's super administrator requires member reset password
SUBCODE_AUTH_EMAIL_CHANGED = 115
SUBCODE_AUTH_SUSPENDED = 116
SUBCODE_AUTH_ACCOUNT_DELETED = 117
SUBCODE_AUTH_OAUTH_AUTHORIZATON_CODE_INVALID = 118
SUBCODE_AUTH_MISSING_ACCESS_TOKEN = 119
SUBCODE_AUTH_INVALID_ACCESS_TOKEN = 120  # incorrect or expired

SUBCODE_DATA_NOT_READY = 121  # when an action is not completed yet, client should wait
SUBCODE_PRIMARY_IDENTITY_UNVERIFIED = 122
# Data management
SUBCODE_NEW_DATA_REQUIRED = 123

# File validation
SUBCODE_FILE_CORRUPTED = 130  # file content is corrupt or mismatched with file extension or file can not be converted
SUBCODE_FILE_BAD_STORAGE = 131  # File is un-readable

# Used for server errors
SUBCODE_SYSTEM_MAINTENANCE = 5031
SUBCODE_SUBSYSTEM_UNREACHABLE = 5032
SUBCODE_EXTERNAL_SYSTEM_UNREACHABLE = 5033  # Google APIs unreachable, Dropbox failed to connect ...
SUBCODE_EXTERNAL_SYSTEM_RETURN_INVALID_DATA = 5033  # Google APIs error, Dropbox error ...


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
