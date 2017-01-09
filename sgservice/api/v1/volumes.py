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

"""The replications api."""

from oslo_config import cfg
from oslo_log import log as logging
import webob


from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.i18n import _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class VolumeViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "volumes"

    def __init__(self):
        """Initialize view builder."""
        super(VolumeViewBuilder, self).__init__()

    def detail(self, request, volume):
        """Detailed view of a single volume."""
        volume_ref = {
            'volume': {
            }
        }
        return volume_ref

    def detail_list(self, request, volumes, volume_count=None):
        """Detailed view of a list of volumes."""
        return self._list_view(self.detail, request, volumes,
                               volume_count,
                               self._collection_name)

    def _list_view(self, func, request, volumes, volume_count,
                   coll_name=_collection_name):
        """Provide a view for a list of volumes.

        :param func: Function used to format the volume data
        :param request: API request
        :param volumes: List of volumes in dictionary format
        :param volume_count: Length of the original list of volumes
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Volume data in dictionary format
        """
        volumes_list = [func(request, volume)['volume'] for
                        volume in volumes]
        volumes_links = self._get_collection_links(request,
                                                   volumes,
                                                   coll_name,
                                                   volume_count)
        volumes_dict = {}
        volumes_dict['volumes'] = volumes_list
        if volumes_links:
            volumes_dict['volumes_links'] = volumes_links

        return volumes_dict


class VolumesController(wsgi.Controller):
    """The Volumes API controller for the SG-Service."""

    _view_builder_class = VolumeViewBuilder

    def __init__(self):
        super(VolumesController, self).__init__()

    def show(self, req, id):
        """Return data about the given volumes."""
        LOG.info(_LI("Show volume with id: %s"), id)
        pass
        return {"volume": {"status": "enabled"}}

    def index(self, req):
        """Returns a list of volumes, transformed through view builder."""
        LOG.info(_LI("Show volume list"))
        return {"volumes": {}}

    @wsgi.action('enable')
    def enable(self, req, id, body):
        """Enable-SG a available volume."""
        LOG.debug('Enable volume SG, volume_id: %s', id)
        pass
        return {"volume": {"status": "enabling"}}

    @wsgi.action('disable')
    def disable(self, req, id, body):
        """Disable a enabled-volume."""
        LOG.info(_LI("Disable volume SG, volume_id: %s"), id)
        pass
        return {"volume": {"status": "disabling"}}

    @wsgi.action('reserve')
    def reserve(self, req, id, body):
        """Mark SG-volume as reserved before attach."""
        LOG.info(_LI("Mark SG-volume as reserved, volume_id: %s"), id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('unreserve')
    def unreserve(self, req, id, body):
        """Unmark volume as reserved."""
        LOG.info(_LI("Unmark volume as reserved, volume_id: %s"), id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('initialize_connection')
    def initialize_connection(self, req, id, body):
        """Initialize volume attachment."""
        pass
        driver_volume_type = "iscsi"
        target_portal = "162.3.117.150:3260"
        target_iqn = "iqn.2016-10.huawei.sg.volume-%s" % id
        target_lun = 1
        data = {
            "target_discovered": False,
            "target_portal": target_portal,
            "target_iqn": target_iqn,
            "target_lun": target_lun,
            "volume_id": id,
            "display_name": id
        }
        connection_info = {
            "driver_volume_type": driver_volume_type,
            "data": data
        }
        return {"connection_info": connection_info}

    @wsgi.action('attach')
    def attach(self, req, id, body):
        """Add sg-volume attachment metadata."""
        LOG.info(_LI("Add SG-volume attachment, volume_id: %s"), id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('begin_detaching')
    def begin_detaching(self, req, id, body):
        """Update volume status to 'detaching'."""
        LOG.info(_LI("Update volume status to detaching, volume_id: %s"),
                 id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('roll_detaching')
    def roll_detaching(self, req, id, body):
        """Roll back volume status to 'in-use'."""
        LOG.info(_LI("Roll back volume status to in-use, volume_id: %s"),
                 id)
        pass
        return webob.Response(status_int=202)

    @wsgi.action('detach')
    def detach(self, req, id, body):
        """Clear attachment metadata."""
        LOG.info(_LI("Clear SG-volume attachment with volume_id: %s"),
                 id)
        pass
        return webob.Response(status_int=202)


def create_resource():
    return wsgi.Resource(VolumesController())
