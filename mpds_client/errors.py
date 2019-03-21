
class APIError(Exception):
    """
    Simple error handling
    """
    codes = {
        400: 'Bad Request',
        401: 'Unauthorized',
        402: 'Unauthorized (Payment Required)',
        403: 'Forbidden',
        404: 'Not Found',
        429: 'Too Many Requests (Rate Limiting)',
        500: 'Internal Server Error',
        503: 'Service Unavailable'
    }

    def __init__(self, msg, code=0):
        Exception.__init__(self)
        self.msg = msg
        self.code = code

    def __str__(self):
        return repr(self.msg + ': ' + self.codes.get(self.code, 'Communication Error'))

