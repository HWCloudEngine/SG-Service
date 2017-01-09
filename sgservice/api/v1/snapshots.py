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

"""The snapshots api."""

from oslo_config import cfg
from oslo_log import log as logging
import webob

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.i18n import _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class SnapshotViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "snapshots"

    def __init__(self):
        """Initialize view builder."""
        super(SnapshotViewBuilder, self).__init__()

    def detail(self, request, snapshot):
        """Detailed view of a single snapshot."""
        snapshot_ref = {
            'snapshot': {
            }
        }
        return snapshot_ref

    def detail_list(self, request, snapshots, snapshot_count=None):
        """Detailed view of a list of snapshots."""
        return self._list_view(self.detail, request, snapshots,
                               snapshot_count,
                               self._collection_name)

    def _list_view(self, func, request, snapshots, snapshot_count,
                   coll_name=_collection_name):
        """Provide a view for a list of snapshots.

        :param func: Function used to format the snapshot data
        :param request: API request
        :param snapshots: List of snapshots in dictionary format
        :param snapshot_count: Length of the original list of snapshots
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Snapshot data in dictionary format
        """
        snapshots_list = [func(request, snapshot)['snapshot'] for
                          snapshot in snapshots]
        snapshots_links = self._get_collection_links(request,
                                                     snapshots,
                                                     coll_name,
                                                     snapshot_count)
        snapshots_dict = {}
        snapshots_dict['snapshots'] = snapshots_list
        if snapshots_links:
            snapshots_dict['snapshots_links'] = snapshots_links

        return snapshots_dict


class SnapshotsController(wsgi.Controller):
    """The Snapshots API controller for the SG-Service."""

    _view_builder_class = SnapshotViewBuilder

    def __init__(self):
        super(SnapshotsController, self).__init__()

    def show(self, req, id):
        """Return data about the given snapshots."""
        LOG.info(_LI("Show snapshot with id: %s"), id)
        pass
        return {"snapshot": {"status": "available"}}

    def delete(self, req, id):
        """Delete a snapshot."""
        LOG.info(_LI("Delete snapshot with id: %s"), id)
        pass
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of snapshots, transformed through view builder."""
        LOG.info(_LI("Show snapshot list"))
        return {"snapshots": {}}

    def create(self, req, body):
        """Creates a new snapshot."""

        LOG.debug('Create snapshot request body: %s', body)
        pass
        return {"snapshot": {"status": "creating"}}

    def update(self, req, id, body):
        """Update a snapshot."""
        LOG.info(_LI("Update snapshot with id: %s"), id)
        pass
        return {"snapshot": {"status": "available"}}


def create_resource():
    return wsgi.Resource(SnapshotsController())
