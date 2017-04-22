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
from oslo_utils import uuidutils
import webob

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.api.v1.volumes import VolumeViewBuilder
from sgservice.common import constants
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ReplicateViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "replicates"

    def __init__(self):
        """Initialize view builder."""
        super(ReplicateViewBuilder, self).__init__()

    def action_summary(self, request, action_info):
        """Generic, non-detailed view of action"""
        replicate_info = {
            'volume_id': action_info['id'],
            'replicate_status': action_info['replicate_status']
        }

        if 'snapshot_id' in action_info.keys():
            replicate_info['snapshot_id'] = action_info['snapshot_id']
        info = {
            'replicate': replicate_info
        }
        return info


class ReplicatesController(wsgi.Controller):
    """The replicates API controller for the SG-Service."""

    _view_builder_class = ReplicateViewBuilder

    def __init__(self):
        self.service_api = ServiceAPI()
        super(ReplicatesController, self).__init__()

    @wsgi.action('delete_replicate')
    def delete_replicate(self, req, id, body):
        """Delete a volume's replicate."""
        LOG.info(_LI("Delete volume's replicate, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.delete_replicate(context, volume)
        return webob.Response(status_int=202)

    @wsgi.action('create_replicate')
    def create_replicate(self, req, id, body):
        """Creates SG-enabled volume's replicate."""
        LOG.debug("Create volume's replicate, volume_id: %s", id)
        context = req.environ['sgservice.context']
        params = body.get('create_replicate', {})
        mode = params.get('mode', constants.REP_MASTER)
        if mode not in constants.SUPPORT_REPLICATE_MODES:
            msg = (_('volume replicate mode must in %s'),
                   constants.SUPPORT_REPLICATE_MODES)
            raise webob.exc.HTTPBadRequest(explanation=msg)

        replication_id = params.get('replication_id', None)
        if replication_id is None:
            msg = _("Replication id can't be None")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        peer_volume = params.get('peer_volume', None)
        if peer_volume is None:
            msg = _("Peer volume can't be None")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        volume = self.service_api.get(context, id)
        replicate_info = self.service_api.create_replicate(
            context, volume, mode, replication_id, peer_volume)
        return self._view_builder.action_summary(req, replicate_info)

    @wsgi.action('enable_replicate')
    def enable_replicate(self, req, id, body):
        """Re-enable a volume's replicate"""
        LOG.info(_LI("Enable volume's replicate, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        replicate_info = self.service_api.enable_replicate(context, volume)
        return self._view_builder.action_summary(req, replicate_info)

    @wsgi.action('disable_replicate')
    def disable_replicate(self, req, id, body):
        """Disable a volume's replicate"""
        LOG.info(_LI("Disable volume's replicate, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        replicate_info = self.service_api.disable_replicate(context, volume)
        return self._view_builder.action_summary(req, replicate_info)

    @wsgi.action('failover_replicate')
    def failover_replicate(self, req, id, body):
        """Failover a volume's replicate"""
        LOG.info(_LI("Failover volume's replicate, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        params = body.get('failover_replicate', {})

        checkpoint_id = params.get('checkpoint_id', None)
        force = params.get('force', False)
        volume = self.service_api.get(context, id)
        replicate_info = self.service_api.failover_replicate(
            context, volume, checkpoint_id, force)
        return self._view_builder.action_summary(req, replicate_info)

    @wsgi.action('reverse_replicate')
    def reverse_replicate(self, req, id, body):
        """Reverse a volume's replicate"""
        LOG.info(_LI("Reverse volume's replicate, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        replicate_info = self.service_api.reverse_replicate(context, volume)
        return self._view_builder.action_summary(req, replicate_info)


def create_resource():
    return wsgi.Resource(ReplicatesController())
