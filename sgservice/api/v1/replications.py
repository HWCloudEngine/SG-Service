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
from webob import exc

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice.objects import fields
from sgservice import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

query_replication_filters_opts = cfg.ListOpt(
    'query_replication_filters',
    default=['name', 'status'],
    help='Replication filter options which non-admin user could use to query '
         'replications.')
CONF.register_opt(query_replication_filters_opts)


class ReplicationViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "replications"

    def __init__(self):
        """Initialize view builder."""
        super(ReplicationViewBuilder, self).__init__()

    def detail(self, request, replication):
        """Detailed view of a single replication."""
        replication_ref = {
            'replication': {
                'id': replication.get('id'),
                'user_id': replication.get('user_id'),
                'status': replication.get('status'),
                'master_volume': replication.get('master_volume'),
                'slave_volume': replication.get('slave_volume'),
                'name': replication.get('display_name'),
                'description': replication.get('display_description')
            }
        }
        return replication_ref

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
        self.service_api = ServiceAPI()
        super(ReplicationsController, self).__init__()

    def _get_replication_filter_options(self):
        return CONF.query_replication_filters

    def show(self, req, id):
        """Return data about the given replications."""
        LOG.info(_LI("Show replication with id: %s"), id)
        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        return self._view_builder.detail(req, replication)

    def delete(self, req, id):
        """Delete a replication."""
        LOG.info(_LI("Delete replication with id: %s"), id)
        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        self.service_api.delete_replication(context, replication)
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of replications, transformed through view builder."""
        LOG.info(_LI("Show replication list"))
        context = req.environ['sgservice.context']
        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        utils.remove_invaild_filter_options(
            context, filters, self._get_replication_filter_options())
        utils.check_filters(filters)

        if 'name' in sort_keys:
            sort_keys[sort_keys.index('name')] = 'display_name'

        if 'name' in filters:
            filters['display_name'] = filters.pop('name')

        replications = self.service_api.get_all_replications(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_replications = self._view_builder.detail_list(req, replications)
        LOG.info(_LI("Show replication list request issued successfully."))
        return retval_replications

    def create(self, req, body):
        """Creates a new replication."""
        if not self.is_valid_body(body, 'replication'):
            raise exc.HTTPUnprocessableEntity()
        LOG.debug('Create replication request body: %s', body)
        context = req.environ['sgservice.context']
        replication = body['replication']

        master_volume_id = replication.get('master_volume', None)
        if master_volume_id is None:
            msg = _('Incorrect request body format')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        slave_volume_id = replication.get('slave_volume', None)
        if slave_volume_id is None:
            msg = _('Incorrect request body format')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        name = replication.get('name', None)
        description = replication.get('description', None)
        if description is None:
            description = 'replication:%s-%s' % (master_volume_id,
                                                 slave_volume_id)

        master_volume = self.service_api.get(context, master_volume_id)
        slave_volume = self.service_api.get(context, slave_volume_id)
        replication = self.service_api.create_replication(
            context, name, description, master_volume, slave_volume)
        return self._view_builder.detail(req, replication)

    def update(self, req, id, body):
        """Update a replication."""
        LOG.info(_LI("Update replication with id: %s"), id)
        context = req.environ['sgservice.context']
        if not body:
            msg = _("Missing request body")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if 'replication' not in body:
            msg = (_("Missing required element '%s' in request body"),
                   'replication')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        replication = body['replication']
        update_dict = {}

        valid_update_keys = (
            'name',
            'description',
            'display_name',
            'display_description',
        )
        for key in valid_update_keys:
            if key in replication:
                update_dict[key] = replication[key]
        self.validate_name_and_description(update_dict)
        if 'name' in update_dict:
            update_dict['display_name'] = update_dict.pop('name')
        if 'description' in update_dict:
            update_dict['display_description'] = update_dict.pop('description')

        replication = self.service_api.get_replication(context, id)
        replication.update(update_dict)
        replication.save()

        return self._view_builder.detail(req, replication)

    @wsgi.action('enable')
    def enable(self, req, id, body):
        """Enable a disabled-replication"""
        LOG.info(_LI("Enable replication with id: %s"), id)
        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        replication = self.service_api.enable_replication(context, replication)
        return self._view_builder.detail(req, replication)

    @wsgi.action('disable')
    def disable(self, req, id, body):
        """Disable a failed-over replication"""
        LOG.info(_LI("Disable replication with id: %s"), id)
        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        replication = self.service_api.disable_replication(context,
                                                           replication)
        return self._view_builder.detail(req, replication)

    @wsgi.action('failover')
    def failover(self, req, id, body):
        """Failover a enabled replication"""
        LOG.info(_LI("Failover replication with id: %s"), id)
        context = req.environ['sgservice.context']

        params = body['failover']
        force = params.get('force', False)
        replication = self.service_api.get_replication(context, id)
        replication = self.service_api.failover_replication(context,
                                                            replication, force)
        return self._view_builder.detail(req, replication)

    @wsgi.action('reverse')
    def reverse(self, req, id, body):
        """reverse a enabled replication"""
        LOG.info(_LI("Reverse replication with id: %s"), id)
        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        replication = self.service_api.reverse_replication(context,
                                                           replication)
        return self._view_builder.detail(req, replication)

    @wsgi.action('reset_status')
    def reset_status(self, req, id, body):
        """reset replication status"""
        LOG.info(_LI("Reset replication status, id: %s"), id)
        status = body['reset_status'].get('status',
                                          fields.ReplicationStatus.ENABLED)
        if status not in fields.ReplicationStatus.ALL:
            msg = _("Invalid status provided.")
            LOG.error(msg)
            raise exception.InvalidStatus(status=status)

        context = req.environ['sgservice.context']
        replication = self.service_api.get_replication(context, id)
        replication.status = status
        replication.save()
        # reset master volume replicate status
        master_volume = self.service_api.get(context,
                                             replication.master_volume)
        if master_volume.replicate_status not in [
            None,
            fields.ReplicateStatus.DELETED
        ]:
            master_volume.replicate_status = status
        master_volume.save()

        # reset slave volume replicate status
        slave_volume = self.service_api.get(context,
                                            replication.slave_volume)
        if slave_volume.replicate_status not in [
            None,
            fields.ReplicateStatus.DELETED
        ]:
            slave_volume.replicate_status = status
        slave_volume.save()
        return webob.Response(status_int=202)


def create_resource():
    return wsgi.Resource(ReplicationsController())
