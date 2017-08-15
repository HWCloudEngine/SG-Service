import webob
from wormhole import exception
from wormhole import wsgi
from wormhole.tasks import addtask
from wormhole.common import utils
from wormhole.common import units

from os_brick.initiator import connector
from oslo_config import cfg
from wormhole.common import log
from wormhole.i18n import _

import functools
import uuid
import os
import ConfigParser

LOG = log.getLogger(__name__)


class SGAgentController(wsgi.Application):
    def __init__(self):
        super(SGAgentController, self).__init__()

    # used the imformation from client and config local information
    def config_sg_client(self, request, sgclient_properties):
        meta_server_ip = sgclient_properties['meta_server_ip']
        config_file = "/etc/storage-gateway/config.ini"
        try:
            config_info = ConfigParser.ConfigParser()
            config_info.read(config_file)

            # modify meta_server_ip
            config_info.set("network", "meta_server_ip", meta_server_ip)
            config_info.write(open(config_file, "w"))

            # check the sg_client run status
            check_run = os.popen(
                "ps -aux | grep sg_client | grep -v grep | cut -c 9-15")
            sg_client_status = check_run.read()
            check_run.close()
            if sg_client_status.strip() != '':
                os.system(
                    "ps -aux | grep sg_client | grep -v grep | cut -c 9-15 | xargs kill -9")

                # start sg_client
            os.popen("/usr/bin/storage-gateway/sg_client &")
            return {"result": "yes"}
        except Exception:
            msg = _("meta_server_ip set up error")
            LOG.debug(msg)
            raise exception.WormholeException

    # used for get sgclient status information
    def get_sg_client_status(self, request, sgclient_properties):
        target_ip = sgclient_properties["meta_server_ip"]
        config_file_url = "/etc/storage-gateway/config.ini"
        try:
            config_info = ConfigParser.ConfigParser()
            config_info.read(config_file_url)
            meta_server_ip = config_info.get("network", "meta_server_ip")

            # check meta_server_ip exsiting or not
            # check target_ip and meta_server_ip relationship
            if meta_server_ip.strip():
                if target_ip != meta_server_ip:
                    return {"status": "no"}
                else:
                    run_list = os.popen(
                        "ps -aux | grep sg_client | grep -v grep | cut -c 9-15")
                    status = run_list.read()
                    run_list.close()
                    if status.strip() != '':
                        return {"status": "yes"}
                    else:
                        return {"status": "no"}
            else:
                return {"status": "no"}
        except Exception:
            msg = _("get sg_client status failed")
            LOG.debug(msg)
            raise exception.WormholeException


def create_router(mapper):
    controller = SGAgentController()

    mapper.connect('/sgagents/config_sg_client',
                   controller=controller,
                   action='config_sg_client',
                   conditions=dict(method=['POST']))

    mapper.connect('/sgagents/get_sg_client_status',
                   controller=controller,
                   action='get_sg_client_status',
                   conditions=dict(method=['POST']))
