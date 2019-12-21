
class APIError(Exception):
    """
    Simple error handling
    """
    codes = {
        204: 'No Results',
        400: 'Bad Request',
        401: 'Unauthorized',
        402: 'Unauthorized (Payment Required)',
        403: 'Forbidden',
        404: 'Not Found',
        413: 'Too Much Data Given',
        429: 'Too Many Requests (Rate Limiting)',
        500: 'Internal Server Error',
        501: 'Not Implemented',
        503: 'Service Unavailable'
    }

    def __init__(self, msg, code=0):
        Exception.__init__(self)
        self.msg = msg
        self.code = code

    def __str__(self):
        return "HTTP error code %s: %s (%s)" % (self.code, self.codes.get(self.code, 'Communication Error'), self.msg)