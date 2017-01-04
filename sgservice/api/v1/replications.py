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

from webob import exc

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.i18n import _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ReplicationViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "replications"

    def __init__(self):
        """Initialize view builder."""
        super(ReplicationViewBuilder, self).__init__()

    def detail(self, request, plan):
        """Detailed view of a single replication."""
        plan_ref = {
            'replication': {
            }
        }
        return plan_ref

    def detail_list(self, request, replications, replication_count=None):
        """Detailed view of a list of replications."""
        return self._list_view(self.detail, request, replications,
                               replication_count,
                               self._collection_name)

    def _list_view(self, func, request, replications, replication_count,
                   coll_name=_collection_name):
        """Provide a view for a list of replications.

        :param func: Function used to format the replication data
        :param request: API request
        :param replications: List of replications in dictionary format
        :param replication_count: Length of the original list of replications
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Replication data in dictionary format
        """
        replications_list = [func(request, replication)['replication'] for
                             replication in replications]
        replications_links = self._get_collection_links(request,
                                                        replications,
                                                        coll_name,
                                                        replication_count)
        replications_dict = {}
        replications_dict['replications'] = replications_list
        if replications_links:
            replications_dict['replications_links'] = replications_links

        return replications_dict


class ReplicationsController(wsgi.Controller):
    """The Replications API controller for the SG-Service."""

    _view_builder_class = ReplicationViewBuilder

    def __init__(self):
        super(ReplicationsController, self).__init__()

    def show(self, req, id):
        """Return data about the given replications."""
        LOG.info(_LI("Show replication with id: %s"), id)
        pass
        return {"replication": {"status": "enabled"}}

    def delete(self, req, id):
        """Delete a replication."""
        LOG.info(_LI("Delete replication with id: %s"), id)
        pass
        return {"replication": {"status": "deleting"}}

    def index(self, req):
        """Returns a list of replications, transformed through view builder."""
        LOG.info(_LI("Show replication list"))
        return {"replications": {}}

    def create(self, req, body):
        """Creates a new replication."""
        if not self.is_valid_body(body, 'replication'):
            raise exc.HTTPUnprocessableEntity()

        LOG.debug('Create replication request body: %s', body)
        pass
        return {"replication": {"status": "enabling"}}

    def update(self, req, replication_id, body):
        """Update a replication."""
        LOG.info(_LI("Update replication with id: %s"), id)
        pass
        return {"replication": {}}

    def enable(self, req, replication_id, body):
        """Enable a disabled-replication"""
        LOG.info(_LI("Enable replication with id: %s"), id)
        pass
        return {"replication": {"status": "enabling"}}

    def disable(self, req, replication_id, body):
        """Disable a failed-over replication"""
        LOG.info(_LI("Disable replication with id: %s"), id)
        pass
        return {"replication": {"status": "disabling"}}

    def failover(self, req, replication_id, body):
        """Failover a enabled replication"""
        LOG.info(_LI("Disable replication with id: %s"), id)
        pass
        return {"replication": {"status": "failing-over"}}

    def reverse(self, req, replication_id, body):
        """reverse a enabled replication"""
        LOG.info(_LI("Reverse replication with id: %s"), id)
        pass
        return {"replication": {"status": "reversing"}}


def create_resource():
    return wsgi.Resource(ReplicationsController())
