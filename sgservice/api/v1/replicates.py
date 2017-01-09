#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""The replicates api."""

from oslo_config import cfg
from oslo_log import log as logging
import webob

from sgservice.api.openstack import wsgi
from sgservice.i18n import _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ReplicatesController(wsgi.Controller):
    """The replicates API controller for the SG-Service."""

    def __init__(self):
        super(ReplicatesController, self).__init__()

    @wsgi.action('delete_replicate')
    def delete_replicate(self, req, id, body):
        """Delete a volume's replicate."""
        LOG.info(_LI("Delete volume's replicate, volume_id: %s"), id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('create_replicate')
    def create(self, req, id, body):
        """Creates SG-enabled volume's replicate."""
        LOG.debug("Create volume's replicate, volume_id: %s", id)
        pass
        return {"volume": {"replicate-status": "enabling"}}

    @wsgi.action('enable_replicate')
    def enable_replicate(self, req, id, body):
        """Re-enable a volume's replicate"""
        LOG.info(_LI("Enable volume's replicate, volume_id: %s"), id)
        pass
        return {"volume": {"replicate-status": "enabling"}}

    @wsgi.action('disable_replicate')
    def disable_replicate(self, req, id, body):
        """Disable a volume's replicate"""
        LOG.info(_LI("Disable volume's replicate, volume_id: %s"), id)
        pass
        return {"volume": {"replicate-status": "disabling"}}

    @wsgi.action('failover_replicate')
    def failover_replicate(self, req, id, body):
        """Failover a volume's replicate"""
        LOG.info(_LI("Failover volume's replicate, volume_id: %s"), id)
        pass
        return {"volume": {"replicate-status": "failing-over"}}

    @wsgi.action('reverse_replicate')
    def reverse_replicate(self, req, id, body):
        """Reverse a volume's replicaten"""
        LOG.info(_LI("Reverse volume's replicate, volume_id: %s"), id)
        pass
        return {"volume": {"replicate-status": "reversing"}}


def create_resource():
    return wsgi.Resource(ReplicatesController())
