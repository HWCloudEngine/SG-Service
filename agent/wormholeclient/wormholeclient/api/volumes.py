class VolumeApiMixin(object):
    def list_volume(self, timeout=10):
        params = {'t': timeout}
        url = self._url("/volumes")
        return self._result(self._get(url, params=params), True)

    def clone_volume(self, volume, src_vref, timeout=10):
        params = {'t': timeout}
        url = self._url("/volumes/clone")
        res = self._post_json(url, data={
            'volume': {'id': volume['id'], 'size': volume['size']},
            'src_vref': {'id': src_vref['id'], 'size': src_vref['size']}})
        return self._result(res, True)

    def remove_device(self, device):
        data = {'device': device}
        url = self._url("/volumes/remove_device")
        res = self._post_json(url, data=data)
        self._raise_for_status(res)
        return res.raw

    def connect_volume(self, connection_properties):
        data = {'connection_properties': connection_properties}
        url = self._url("/volumes/connect_volume")
        res = self._post_json(url, data=data)
        return self._result(res, True)

    def disconnect_volume(self, connection_properties):
        data = {'connection_properties': connection_properties}
        url = self._url("/volumes/disconnect_volume")
        res = self._post_json(url, data=data)
        self._raise_for_status(res)
        return res.raw
