import requests
import requests.exceptions
from .. import utils
from .. import errors


class ContainerApiMixin(object):

    def create_container(self, image_name, image_id, root_volume_id=None, network_info=None, block_device_info=None,
                         inject_files=None, admin_password=None, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/create")
        create_config = utils.create_container_config(image_name, image_id,
                            root_volume_id=root_volume_id,
                            network_info=network_info,
                            block_device_info=block_device_info,
                            inject_files=inject_files,
                            admin_password=admin_password)
        res = self._post_json(url, params=params, data=create_config)
        create_task = self._result(res, True)
        return create_task

    def restart_container(self, timeout=10, network_info=None, block_device_info=None):
        params = {'t': timeout}
        url = self._url("/container/restart")
        restart_config = utils.restart_container_config(network_info, block_device_info)
        res = self._post_json(url, params=params, data=restart_config)
        self._raise_for_status(res)
        return res.raw

    def stop_container(self, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/stop")
        res = self._post_json(url, params=params, data=None)
        self._raise_for_status(res)
        return res.raw

    def start_container(self, timeout=10, network_info=None, block_device_info=None):
        params = {'t': timeout}
        url = self._url("/container/start")
        start_config = utils.start_container_config(network_info, block_device_info)
        res = self._post_json(url, params=params, data=start_config)
        self._raise_for_status(res)
        return res.raw

    def inject_files(self, inject_files, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/inject-files")
        res = self._post_json(url, data={ 'inject_files' : inject_files })
        self._raise_for_status(res)
        return res.raw

    def pause_container(self, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/pause")
        res = self._post_json(url, params=params, data=None)
        self._raise_for_status(res)
        return res.raw

    def unpause_container(self, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/unpause")
        res = self._post_json(url, params=params, data=None)
        self._raise_for_status(res)
        return res.raw

    def get_console_output(self, timeout=10):
        """ return { "logs": "the log text of container" }
        """
        params = {'t': timeout}
        url = self._url("/container/console-output")
        return self._result(self._get(url, params=params), True)

    def set_admin_password(self, admin_password, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/admin-password")
        admin_password_config = utils.admin_password_config(admin_password)
        res = self._post_json(url, params=params, data=admin_password_config)
        self._raise_for_status(res)
        return res.raw

    def create_image(self, image_name, image_id, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/create-image")
        create_image_config = utils.create_image_config(image_name, image_id)
        res = self._post_json(url, params=params, data=create_image_config)
        return  self._result(res, True)

    def attach_volume(self, volume_id, device, mount_device, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/attach-volume")
        res = self._post_json(url, data={ 'volume' : volume_id, 'device': device, 'mount_device' : mount_device })
        self._raise_for_status(res)
        return res.raw

    def detach_volume(self, volume_id, timeout=10):
        params = {'t': timeout}
        url = self._url("/container/detach-volume")
        res = self._post_json(url, data={ 'volume' : volume_id })
        self._raise_for_status(res)
        return res.raw

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
    
    def status(self):
        """ Query Container status.
            return: { "status": "Up 6 days"}
        """
        url = self._url("/container/status")
        status = self._result(self._get(url), True)
        return status


    def image_info(self, image_name, image_id):
        """ Query Container status.
            return: {
                     "size": 253283606,
                     "name": "ubuntu-docker",
                     "id": "eee8ccb1-b3f7-4b9c-9756-7437d1793fa5"
                     }

        """
        url = self._url("/container/image-info?image_id=%s&image_name=%s" % (image_id, image_name))
        image_info = self._result(self._get(url), True)
        return image_info

