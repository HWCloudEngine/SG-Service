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
from oslo_utils import uuidutils
import webob

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

SUPPORT_BACKUP_TYPE = ['full', 'incremental']
SUPPORT_BACKUP_DESTINATION = ['local', 'remote']


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
                'id': backup.get('id'),
                'status': backup.get('status'),
                'availability_zone': backup.get('availability_zone'),
                'type': backup.get('type'),
                'destination': backup.get('destination'),
                'volume_id': backup.get('volume_id'),
            }
        }
        return backup_ref

    def restore_summary(self, request, restore):
        """Generic, non-detailed view of restore"""
        return {
            'restore': {
                'backup_id': restore['backup_id'],
                'volume_id': restore['volume_id'],
                'volume_name': restore['volume_name']
            }
        }

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
        self.service_api = ServiceAPI()
        super(BackupsController, self).__init__()

    def show(self, req, id):
        """Return data about the given backups."""
        LOG.info(_LI("Show backup, backup_id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid backup id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        return self._view_builder.detail(req, backup)

    def delete(self, req, id):
        """Delete a backup."""
        LOG.info(_LI("Delete backup, backup_id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid backup id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        self.service_api.delete_backup(context, backup)
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of backups, transformed through view builder."""
        LOG.info(_LI("Show backup list"))
        context = req.environ['sgservice.context']
        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        utils.remove_invaild_filter_options(
            context, filters, self._get_replication_filter_options())
        utils.check_filters(filters)

        backups = self.service_api.get_all_backups(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_backups = self._view_builder.detail_list(req, backups)
        LOG.info(_LI("Show backup list request issued successfully."))
        return retval_backups

    def create(self, req, body):
        """Creates a new backup."""
        LOG.debug('Create backup request body: %s', body)
        context = req.environ['sgservice.context']
        backup = body['backup']

        volume_id = backup.get('volume_id', None)
        if volume_id is None:
            msg = _('Incorrect request body format')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        name = backup.get('name', 'backup-%s' % volume_id)
        description = backup.get('description', name)

        backup_type = backup.get('type', 'incremental')
        if backup_type not in SUPPORT_BACKUP_TYPE:
            msg = _('backup type should be full or incremental')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        backup_destination = backup.get('destination', 'local')
        if backup_destination not in SUPPORT_BACKUP_DESTINATION:
            msg = _('backup destination should be local or remote')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        if not uuidutils.is_uuid_like(volume_id):
            msg = _("Invalid volume id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(volume_id)

        volume = self.service_api.get(context, volume_id)
        backup = self.service_api.create_backup(context, name, description,
                                                volume, backup_type,
                                                backup_destination)
        return self._view_builder.detail(req, backup)

    def update(self, req, id, body):
        """Update a backup."""
        LOG.info(_LI("Update backup, backup_id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid backup id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)

        # TODO(luobin): implement update backup [status, name, description]
        return self._view_builder.detail(req, backup)

    def restore(self, req, id, body):
        """Restore backup to an SG-enabled volume"""
        LOG.info(_LI("Restore backup to sg-enabled volume, backup_id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid backup id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        restore = body['restore']
        volume_id = restore.get('volume_id', None)
        if volume_id is None:
            msg = _('restored volume should be specified.')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        restore = self.service_api.restore_backup(context, backup, volume_id)
        return self._view_builder.restore_summary(req, restore)


def create_resource():
    return wsgi.Resource(BackupsController())
