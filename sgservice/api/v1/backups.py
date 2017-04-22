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
from sgservice.common import constants
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice.objects import fields
from sgservice import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

query_backup_filters_opts = cfg.ListOpt(
    'query_backup_filters',
    default=['name', 'status', 'volume_id'],
    help='Backup filter options which non-admin user could use to query '
         'backups.')
CONF.register_opt(query_backup_filters_opts)


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

    def export_summary(self, request, export):
        """Generic view of an export."""
        return {
            'backup_record': {
                'driver_data': export['driver_data'],
                'backup_type': export['backup_type'],
                'availability_zone': export['availability_zone']
            },
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

    def _get_backup_filter_options(self):
        return CONF.query_backup_filters

    def show(self, req, id):
        """Return data about the given backups."""
        LOG.info(_LI("Show backup, backup_id: %s"), id)
        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        return self._view_builder.detail(req, backup)

    def delete(self, req, id):
        """Delete a backup."""
        LOG.info(_LI("Delete backup, backup_id: %s"), id)
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
            context, filters, self._get_backup_filter_options())
        utils.check_filters(filters)

        if 'name' in sort_keys:
            sort_keys[sort_keys.index('name')] = 'display_name'

        if 'name' in filters:
            filters['display_name'] = filters.pop('name')

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

        name = backup.get('name', None)
        description = backup.get('description', None)
        if description is None:
            description = 'backup-%s' % volume_id

        backup_type = backup.get('type')
        if backup_type is None:
            backup_type = constants.FULL_BACKUP
        if backup_type not in constants.SUPPORT_BACKUP_TYPES:
            msg = _('backup type should be full or incremental')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        backup_destination = backup.get('destination')
        if backup_destination is None:
            backup_destination = constants.LOCAL_BACKUP
        if backup_destination not in constants.SUPPORT_BACKUP_DESTINATIONS:
            msg = _('backup destination should be local or remote')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        volume = self.service_api.get(context, volume_id)
        backup = self.service_api.create_backup(context, name, description,
                                                volume, backup_type,
                                                backup_destination)
        return self._view_builder.detail(req, backup)

    def update(self, req, id, body):
        """Update a backup."""
        LOG.info(_LI("Update backup, backup_id: %s"), id)
        context = req.environ['sgservice.context']
        if not body:
            msg = _("Missing request body")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if 'backup' not in body:
            msg = (_("Missing required element '%s' in request body"),
                   'backup')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        backup = body['backup']
        update_dict = {}

        valid_update_keys = (
            'name',
            'description',
            'display_name',
            'display_description',
        )
        for key in valid_update_keys:
            if key in backup:
                update_dict[key] = backup[key]
        self.validate_name_and_description(update_dict)
        if 'name' in update_dict:
            update_dict['display_name'] = update_dict.pop('name')
        if 'description' in update_dict:
            update_dict['display_description'] = update_dict.pop('description')

        backup = self.service_api.get_backup(context, id)
        backup.update(update_dict)
        backup.save()
        return self._view_builder.detail(req, backup)

    @wsgi.action('restore')
    def restore(self, req, id, body):
        """Restore backup to an SG-enabled volume"""
        LOG.info(_LI("Restore backup to sg-enabled volume, backup_id: %s"), id)
        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        restore = body['restore']
        volume_id = restore.get('volume_id', None)
        if volume_id is None:
            msg = _('restored volume should be specified.')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        restore = self.service_api.restore_backup(context, backup, volume_id)
        return self._view_builder.restore_summary(req, restore)

    @wsgi.action('export_record')
    def export_record(self, req, id, body):
        """Export backup record"""
        LOG.info(_LI("Export backup record, backup_id: %s"), id)
        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)

        record = self.service_api.export_record(context, backup)
        return self._view_builder.export_summary(req, record)

    def import_record(self, req, body):
        """Import a backup"""
        LOG.info(_LI("Importing record from body: %s"), body)
        self.assert_valid_body(body, 'backup_record')
        context = req.environ['sgservice.context']
        backup_record = body['backup_record']
        # Verify that body elements are provided
        try:
            driver_data = backup_record['driver_data']
        except KeyError:
            msg = _("Incorrect request body format.")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        LOG.debug('Importing backup using driver_data %s.', driver_data)

        try:
            new_backup = self.service_api.import_record(context, backup_record)
        except exception.InvalidBackup as error:
            raise webob.exc.HTTPBadRequest(explanation=error.msg)
        # Other Not found exceptions will be handled at the wsgi level
        except exception.ServiceNotFound as error:
            raise webob.exc.HTTPInternalServerError(explanation=error.msg)

        retval = self._view_builder.detail(req, new_backup)
        return retval

    @wsgi.action('reset_status')
    def reset_status(self, req, id, body):
        """reset backup status"""
        LOG.info(_LI("Reset backup status, id: %s"), id)
        status = body['reset_status'].get('status',
                                          fields.BackupStatus.AVAILABLE)
        if status not in fields.BackupStatus.ALL:
            msg = _("Invalid status provided.")
            LOG.error(msg)
            raise exception.InvalidStatus(status=status)

        context = req.environ['sgservice.context']
        backup = self.service_api.get_backup(context, id)
        backup.status = status
        backup.save()
        return webob.Response(status_int=202)


def create_resource():
    return wsgi.Resource(BackupsController())
