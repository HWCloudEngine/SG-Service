import requests


class APIError(requests.exceptions.HTTPError):
    def __init__(self, message, response, explanation=None):
        # requests 1.2 supports response as a keyword argument, but
        # requests 1.1 doesn't
        super(APIError, self).__init__(message)
        self.response = response

        self.explanation = explanation

        if self.explanation is None and response.content:
            self.explanation = response.content.strip()

    def __str__(self):
        message = super(APIError, self).__str__()

        if self.is_client_error():
            message = '{0} Client Error: {1}'.format(
                self.response.status_code, self.response.reason)

        elif self.is_server_error():
            message = '{0} Server Error: {1}'.format(
                self.response.status_code, self.response.reason)

        if self.explanation:
            message = '{0} ("{1}")'.format(message, self.explanation)

        return message

    def is_client_error(self):
        return 400 <= self.response.status_code < 500

    def is_server_error(self):
        return 500 <= self.response.status_code < 600


class HyperServiceException(Exception):
    pass


class NotFound(APIError):
    pass


class InvalidBaseUrl(HyperServiceException):
    pass

class ConnectionError(HyperServiceException):
    pass

class InternalError(HyperServiceException):
    pass

class InvalidHost(HyperServiceException):
    pass


class InvalidConfigFile(HyperServiceException):
    pass


class DeprecatedMethod(HyperServiceException):
    pass


class NullResource(HyperServiceException, ValueError):
    pass
