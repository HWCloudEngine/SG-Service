import requests
import requests.exceptions
from .. import errors


class VersionApiMixin(object):
    def get_version(self, timeout=10):
        params = {'t': timeout}
        url = self._url("/", versioned_api=False)
        try:
            return self._result(self._get(url, params=params), True)
        except requests.exceptions.ConnectionError as ce:
            raise errors.ConnectionError()
        except Exception as e:
            raise errors.InternalError()
