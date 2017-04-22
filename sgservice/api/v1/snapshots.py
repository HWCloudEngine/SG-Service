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
from oslo_utils import uuidutils
import webob

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice.objects import fields
from sgservice import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

query_snapshot_filters_opts = cfg.ListOpt(
    'query_snapshot_filters',
    default=['name', 'status', 'volume_id'],
    help='Snapshot filter options which non-admin user could use to query '
         'snapshots.')
CONF.register_opt(query_snapshot_filters_opts)


class SnapshotViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "snapshots"

    def __init__(self):
        """Initialize view builder."""
        super(SnapshotViewBuilder, self).__init__()

    def rollback_summary(self, request, rollback):
        """summary view of rollback"""
        rollback_ref = {
            'rollback': {
                'id': rollback.get('id'),
                'volume_id': rollback.get('volume_id'),
                'volume_status': rollback.get('volume_status')
            }
        }
        return rollback_ref

    def detail(self, request, snapshot):
        """Detailed view of a single snapshot."""
        snapshot_ref = {
            'snapshot': {
                'id': snapshot.id,
                'name': snapshot.display_name,
                'description': snapshot.display_description,
                'volume_id': snapshot.volume_id,
                'status': snapshot.status,
                'checkpoint_id': snapshot.checkpoint_id
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
        self.service_api = ServiceAPI()
        super(SnapshotsController, self).__init__()

    def _get_snapshot_filter_options(self):
        return CONF.query_snapshot_filters

    def show(self, req, id):
        """Return data about the given snapshots."""
        LOG.info(_LI("Show snapshot with id: %s"), id)
        context = req.environ['sgservice.context']
        snapshot = self.service_api.get_snapshot(context, id)
        return self._view_builder.detail(req, snapshot)

    def delete(self, req, id):
        """Delete a snapshot."""
        LOG.info(_LI("Delete snapshot with id: %s"), id)
        context = req.environ['sgservice.context']
        snapshot = self.service_api.get_snapshot(context, id)
        self.service_api.delete_snapshot(context, snapshot)
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of snapshots, transformed through view builder."""
        LOG.info(_LI("Show snapshot list"))
        context = req.environ['sgservice.context']
        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        utils.remove_invaild_filter_options(
            context, filters, self._get_snapshot_filter_options())
        utils.check_filters(filters)

        if 'name' in sort_keys:
            sort_keys[sort_keys.index('name')] = 'display_name'

        if 'name' in filters:
            filters['display_name'] = filters.pop('name')

        snapshots = self.service_api.get_all_snapshots(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_snapshots = self._view_builder.detail_list(req, snapshots)
        LOG.info(_LI("Show snapshot list request issued successfully."))
        return retval_snapshots

    def create(self, req, body):
        """Creates a new snapshot."""
        LOG.debug('Create snapshot request body: %s', body)
        context = req.environ['sgservice.context']
        snapshot = body['snapshot']

        volume_id = snapshot.get('volume_id', None)
        if volume_id is None:
            msg = _('Incorrect request body format')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        name = snapshot.get('name', None)
        description = snapshot.get('description', None)
        if description is None:
            description = 'snapshot-%s' % volume_id
        checkpoint_id = snapshot.get('checkpoint_id', None)

        volume = self.service_api.get(context, volume_id)
        snapshot = self.service_api.create_snapshot(
            context, name, description, volume, checkpoint_id)
        return self._view_builder.detail(req, snapshot)

    def update(self, req, id, body):
        """Update a snapshot."""
        LOG.info(_LI("Update snapshot with id: %s"), id)
        context = req.environ['sgservice.context']
        if not body:
            msg = _("Missing request body")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if 'snapshot' not in body:
            msg = (_("Missing required element '%s' in request body"),
                   'snapshot')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        snapshot = body['snapshot']
        update_dict = {}

        valid_update_keys = (
            'name',
            'description',
            'display_name',
            'display_description',
        )
        for key in valid_update_keys:
            if key in snapshot:
                update_dict[key] = snapshot[key]
        self.validate_name_and_description(update_dict)
        if 'name' in update_dict:
            update_dict['display_name'] = update_dict.pop('name')
        if 'description' in update_dict:
            update_dict['display_description'] = update_dict.pop('description')

        snapshot = self.service_api.get_snapshot(context, id)
        snapshot.update(update_dict)
        snapshot.save()
        return self._view_builder.detail(req, snapshot)

    @wsgi.action('rollback')
    def rollback(self, req, id, body):
        """Rollback a snapshot to volume"""
        LOG.info(_LI("Rollback snapshot with id: %s"), id)
        context = req.environ['sgservice.context']
        snapshot = self.service_api.get_snapshot(context, id)
        rollback = self.service_api.rollback_snapshot(context, snapshot)
        return self._view_builder.rollback_summary(req, rollback)

    @wsgi.action('reset_status')
    def reset_status(self, req, id, body):
        """reset snapshot status"""
        LOG.info(_LI("Reset snapshot status, id: %s"), id)
        status = body['reset_status'].get('status',
                                          fields.SnapshotStatus.AVAILABLE)
        if status not in fields.SnapshotStatus.ALL:
            msg = _("Invalid status provided.")
            LOG.error(msg)
            raise exception.InvalidStatus(status=status)

        context = req.environ['sgservice.context']
        snapshot = self.service_api.get_snapshot(context, id)
        snapshot.status = status
        snapshot.save()
        return webob.Response(status_int=202)


def create_resource():
    return wsgi.Resource(SnapshotsController())
