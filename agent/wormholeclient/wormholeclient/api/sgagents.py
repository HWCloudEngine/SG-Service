class SGAgentApiMixin(object):
    def config_sg_client(self, sgclient_properties):
        data = {'sgclient_properties': sgclient_properties}
        url = self._url("/sgagents/config_sg_client")
        res = self._post_json(url, data=data)
        return self._result(res, True)

    def get_sg_client_status(self, sgclient_properties):
        data = {'sgagent_properties': sgclient_properties}
        url = self._url("/sgagents/get_sg_client_status")
        res = self._post_json(url, data=data)
        self._raise_for_status(res)
        return self._result(res, True)
