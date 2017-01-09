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

"""The backups api."""

from oslo_config import cfg
from oslo_log import log as logging
import webob

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.i18n import _LI

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class BackupViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "backups"

    def __init__(self):
        """Initialize view builder."""
        super(BackupViewBuilder, self).__init__()

    def detail(self, request, backup):
        """Detailed view of a single backup."""
        backup_ref = {
            'backup': {
            }
        }
        return backup_ref

    def detail_list(self, request, backups, backup_count=None):
        """Detailed view of a list of backups."""
        return self._list_view(self.detail, request, backups,
                               backup_count,
                               self._collection_name)

    def _list_view(self, func, request, backups, backup_count,
                   coll_name=_collection_name):
        """Provide a view for a list of backups.

        :param func: Function used to format the backup data
        :param request: API request
        :param backups: List of backups in dictionary format
        :param backup_count: Length of the original list of backups
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Backup data in dictionary format
        """
        backups_list = [func(request, backup)['backup'] for
                        backup in backups]
        backups_links = self._get_collection_links(request,
                                                   backups,
                                                   coll_name,
                                                   backup_count)
        backups_dict = {}
        backups_dict['backups'] = backups_list
        if backups_links:
            backups_dict['backups_links'] = backups_links

        return backups_dict


class BackupsController(wsgi.Controller):
    """The Backups API controller for the SG-Service."""

    _view_builder_class = BackupViewBuilder

    def __init__(self):
        super(BackupsController, self).__init__()

    def show(self, req, id):
        """Return data about the given backups."""
        LOG.info(_LI("Show backup, backup_id: %s"), id)
        pass
        return {"backup": {"status": "available"}}

    def delete(self, req, id):
        """Delete a backup."""
        LOG.info(_LI("Delete backup, backup_id: %s"), id)
        pass
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of backups, transformed through view builder."""
        LOG.info(_LI("Show backup list"))
        return {"backups": {}}

    def create(self, req, body):
        """Creates a new backup."""

        LOG.debug('Create backup request body: %s', body)
        pass
        return {"backup": {"status": "creating"}}

    def update(self, req, id, body):
        """Update a backup."""
        LOG.info(_LI("Update backup, backup_id: %s"), id)
        pass
        return {"backup": {"status": "available"}}

    def restore(self, req, id, body):
        """Restore backup to an SG-enabled volume"""
        LOG.info(_LI("Restore backup to sg-enabled volume, backup_id: %s"), id)
        pass
        return {"backup": {"status": "restoring"}}


def create_resource():
    return wsgi.Resource(BackupsController())
