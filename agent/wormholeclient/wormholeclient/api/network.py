class NetworkApiMixin(object):

    def attach_interface(self, vif, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/attach-interface")
        res = self._post_json(url, data={ 'vif' : vif })
        self._raise_for_status(res)
        return res.raw

    def detach_interface(self, vif, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/detach-interface")
        res = self._post_json(url, data={ 'vif' : vif })
        self._raise_for_status(res)
        return res.raw

