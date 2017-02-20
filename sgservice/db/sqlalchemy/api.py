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

"""Implementation of SQLAlchemy backend."""

import functools
import re
import six
import sys
import threading
import time
import uuid

from oslo_config import cfg
from oslo_db import exception as db_exc
from oslo_db import options
from oslo_db.sqlalchemy import session as db_session
from oslo_db.sqlalchemy import utils as sqlalchemyutils
from oslo_log import log as logging
from oslo_utils import timeutils
from oslo_utils import uuidutils
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql import func

from sgservice.db.sqlalchemy import models
from sgservice import exception
from sgservice.i18n import _, _LW
from sgservice.objects import fields

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

options.set_defaults(CONF, connection='sqlite:///$state_path/sgservice.sqlite')

_LOCK = threading.Lock()
_FACADE = None
_GET_METHODS = {}


def _create_facade_lazily():
    global _LOCK
    with _LOCK:
        global _FACADE
        if _FACADE is None:
            _FACADE = db_session.EngineFacade(
                CONF.database.connection,
                **dict(CONF.database)
            )

        return _FACADE


def get_engine():
    facade = _create_facade_lazily()
    return facade.get_engine()


def get_session(**kwargs):
    facade = _create_facade_lazily()
    return facade.get_session(**kwargs)


def dispose_engine():
    get_engine().dispose()


_DEFAULT_QUOTA_NAME = 'default'


def get_backend():
    """The backend is this module itself."""

    return sys.modules[__name__]


def is_admin_context(context):
    """Indicates if the request context is an administrator."""
    if not context:
        LOG.warning(_LW('Use of empty request context is deprecated'),
                    DeprecationWarning)
        raise Exception('die')
    return context.is_admin


def is_user_context(context):
    """Indicates if the request context is a normal user."""
    if not context:
        return False
    if context.is_admin:
        return False
    if not context.user_id or not context.project_id:
        return False
    return True


def authorize_project_context(context, project_id):
    """Ensures a request has permission to access the given project."""
    if is_user_context(context):
        if not context.project_id:
            raise exception.NotAuthorized()
        elif context.project_id != project_id:
            raise exception.NotAuthorized()


def authorize_user_context(context, user_id):
    """Ensures a request has permission to access the given user."""
    if is_user_context(context):
        if not context.user_id:
            raise exception.NotAuthorized()
        elif context.user_id != user_id:
            raise exception.NotAuthorized()


def require_admin_context(f):
    """Decorator to require admin request context.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]):
            raise exception.AdminRequired()
        return f(*args, **kwargs)

    return wrapper


def require_context(f):
    """Decorator to require *any* user or admin context.

    This does no authorization for user or project access matching, see
    :py:func:`authorize_project_context` and
    :py:func:`authorize_user_context`.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]) and not is_user_context(args[0]):
            raise exception.NotAuthorized()
        return f(*args, **kwargs)

    return wrapper


def require_volume_exists(f):
    """Decorator to require the specified plan to exist.

    Requires the wrapped function to use context and plan_id as
    their first two arguments.
    """

    @functools.wraps(f)
    def wrapper(context, volume_id, *args, **kwargs):
        volume_get(context, volume_id)
        return f(context, volume_id, *args, **kwargs)

    return wrapper


def _retry_on_deadlock(f):
    """Decorator to retry a DB API call if Deadlock was received."""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except db_exc.DBDeadlock:
                LOG.warning(_LW("Deadlock detected when running "
                                "'%(func_name)s': Retrying..."),
                            dict(func_name=f.__name__))
                # Retry!
                time.sleep(0.5)
                continue

    functools.update_wrapper(wrapped, f)
    return wrapped


def handle_db_data_error(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except db_exc.DBDataError:
            msg = _('Error writing field to database')
            LOG.exception(msg)
            raise exception.Invalid(msg)

    return wrapper


def model_query(context, *args, **kwargs):
    """Query helper that accounts for context's `read_deleted` field.

    :param context: context to query under
    :param session: if present, the session to use
    :param read_deleted: if present, overrides context's read_deleted field.
    :param project_only: if present and context is user-type, then restrict
            query to match the context's project_id.
    """
    session = kwargs.get('session') or get_session()
    read_deleted = kwargs.get('read_deleted') or context.read_deleted
    project_only = kwargs.get('project_only')

    query = session.query(*args)
    if read_deleted == 'no':
        query = query.filter_by(deleted=False)
    elif read_deleted == 'yes':
        pass  # omit the filter to include deleted and active
    elif read_deleted == 'only':
        query = query.filter_by(deleted=True)
    else:
        raise Exception(
            _("Unrecognized read_deleted value '%s'") % read_deleted)

    if project_only and is_user_context(context):
        query = query.filter_by(project_id=context.project_id)

    return query


def is_valid_model_filters(model, filters):
    """Return True if filter values exist on the model

    :param model: a sgservice model
    :param filters: dictionary of filters
    """
    for key in filters.keys():
        try:
            getattr(model, key)
        except AttributeError:
            LOG.debug("'%s' filter key is not valid.", key)
            return False
    return True


@require_admin_context
@_retry_on_deadlock
def service_destroy(context, service_id):
    session = get_session()
    updated_values = models.Service.delete_values()
    with session.begin():
        service_ref = _service_get(context, service_id, session=session)
        service_ref.update(updated_values)
        service_ref.save(session)
    return updated_values


@require_admin_context
def _service_get(context, service_id, session=None):
    result = model_query(
        context,
        models.Service,
        session=session). \
        filter_by(id=service_id). \
        first()
    if not result:
        raise exception.ServiceNotFound(service_id=service_id)

    return result


@require_admin_context
def service_get(context, service_id):
    return _service_get(context, service_id)


@require_admin_context
def service_get_all(context, disabled=None):
    query = model_query(context, models.Service)

    if disabled is not None:
        query = query.filter_by(disabled=disabled)

    return query.all()


@require_admin_context
def service_get_all_by_topic(context, topic, disabled=None):
    query = model_query(
        context, models.Service, read_deleted="no"). \
        filter_by(topic=topic)

    if disabled is not None:
        query = query.filter_by(disabled=disabled)

    return query.all()


@require_admin_context
def service_get_by_host_and_topic(context, host, topic):
    result = model_query(
        context, models.Service, read_deleted="no"). \
        filter_by(disabled=False). \
        filter_by(host=host). \
        filter_by(topic=topic). \
        first()
    if not result:
        raise exception.ServiceNotFound(service_id=None)
    return result


@require_admin_context
def _service_get_all_topic_subquery(context, session, topic, subq, label):
    sort_value = getattr(subq.c, label)
    return model_query(context, models.Service,
                       func.coalesce(sort_value, 0),
                       session=session, read_deleted="no"). \
        filter_by(topic=topic). \
        filter_by(disabled=False). \
        outerjoin((subq, models.Service.host == subq.c.host)). \
        order_by(sort_value). \
        all()


@require_admin_context
def service_get_by_args(context, host, binary):
    results = model_query(context, models.Service). \
        filter_by(host=host). \
        filter_by(binary=binary). \
        all()

    for result in results:
        if host == result['host']:
            return result

    raise exception.HostBinaryNotFound(host=host, binary=binary)


@require_admin_context
def service_create(context, values):
    service_ref = models.Service()
    service_ref.update(values)
    if not CONF.enable_new_services:
        service_ref.disabled = True

    session = get_session()
    with session.begin():
        service_ref.save(session)
        return service_ref


@handle_db_data_error
@require_admin_context
def service_update(context, service_id, values):
    session = get_session()
    with session.begin():
        service_ref = _service_get(context, service_id, session=session)
        if 'disabled' in values:
            service_ref['modified_at'] = timeutils.utcnow()
            service_ref['updated_at'] = literal_column('updated_at')
        service_ref.update(values)
        return service_ref


def _get_get_method(model):
    # General conversion
    # Convert camel cased model name to snake format
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', model.__name__)
    # Get method must be snake formatted model name concatenated with _get
    method_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower() + '_get'
    return globals().get(method_name)


@require_context
def get_by_id(context, model, id, *args, **kwargs):
    # Add get method to cache dictionary if it's not already there
    if not _GET_METHODS.get(model):
        _GET_METHODS[model] = _get_get_method(model)

    return _GET_METHODS[model](context, id, *args, **kwargs)


##################


@require_context
def _list_common_get_query(context, model, session=None):
    return model_query(context, model, session=session)


def _list_common_process_exact_filter(model, query, filters, legal_keys):
    """Applies exact match filtering to a query.

    :param model: model to apply filters to
    :param query: query to apply filters to
    :param filters: dictionary of filters; values that are lists,
                    tuples, sets, or frozensets cause an 'IN' test to
                    be performed, while exact matching ('==' operator)
                    is used for other values
    :param legal_keys: list of keys to apply exact filtering to
    :returns: the updated query.
    """

    filter_dict = {}
    for key in legal_keys:
        if key not in filters:
            continue

        value = filters.get(key)
        if isinstance(value, (list, tuple, set, frozenset)):
            if not value:
                return None  # empty IN-predicate; short circuit
            # Looking for values in a list; apply to query directly
            column_attr = getattr(model, key)
            query = query.filter(column_attr.in_(value))
        else:
            # OK, simple exact match; save for later
            filter_dict[key] = value

    # Apply simple exact matches
    if filter_dict:
        query = query.filter_by(**filter_dict)

    return query


def _list_common_process_regex_filter(model, query, filters, legal_keys):
    """Applies regular expression filtering to a query.

    :param model: model to apply filters to
    :param query: query to apply filters to
    :param filters: dictionary of filters with regex values
    :param legal_keys: list of keys to apply regex filtering to
    :returns: the updated query.
    """

    def _get_regexp_op_for_connection(db_connection):
        db_string = db_connection.split(':')[0].split('+')[0]
        regexp_op_map = {
            'postgresql': '~',
            'mysql': 'REGEXP',
            'sqlite': 'REGEXP'
        }
        return regexp_op_map.get(db_string, 'LIKE')

    db_regexp_op = _get_regexp_op_for_connection(CONF.database.connection)
    for key in legal_keys:
        if key not in filters:
            continue

        value = filters[key]
        if not isinstance(value, six.string_types):
            continue

        column_attr = getattr(model, key)
        if db_regexp_op == 'LIKE':
            query = query.filter(column_attr.op(db_regexp_op)(
                u'%' + value + u'%'))
        else:
            query = query.filter(column_attr.op(db_regexp_op)(
                value))
    return query


#################


@require_context
def _volume_get_query(context, columns_to_join=[], session=None,
                      project_only=False, read_deleted=None):
    if columns_to_join and 'replication' in columns_to_join:
        return model_query(context, models.Volume, session=session,
                           project_only=project_only,
                           read_deleted=read_deleted). \
            options(joinedload('volume_attachment')). \
            options(joinedload('replication'))

    return model_query(context, models.Volume, session=session,
                       project_only=project_only,
                       read_deleted=read_deleted). \
        options(joinedload('volume_attachment'))


@require_context
def _volume_get(context, volume_id, columns_to_join=[], session=None,
                read_deleted=None):
    result = _volume_get_query(context, columns_to_join=columns_to_join,
                               session=session, project_only=True,
                               read_deleted=read_deleted)

    result = result.filter_by(id=volume_id).first()
    if not result:
        raise exception.VolumeNotFound(volume_id=volume_id)

    return result


def _process_volume_filters(query, filters):
    if filters:
        if not is_valid_model_filters(models.Volume, filters):
            return
        query = query.filter_by(**filters)
    return query


@require_context
def volume_create(context, values):
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    volume_ref = models.Volume()
    volume_ref.update(values)

    session = get_session()
    with session.begin():
        session.add(volume_ref)

    return _volume_get(context, values['id'], session=session)


@require_context
def volume_reenable(context, volume_id, values):
    session = get_session()
    values['deleted'] = False
    values['deleted_at'] = None
    with session.begin():
        volume_ref = _volume_get(context, volume_id, session=session,
                                 read_deleted='yes')
        volume_ref.update(values)
        volume_ref.save(session)
    return volume_ref


@handle_db_data_error
@require_context
def volume_update(context, volume_id, values):
    session = get_session()
    with session.begin():
        volume_ref = _volume_get(context, volume_id, session=session,
                                 read_deleted='yes')
        volume_ref.update(values)
        volume_ref.save(session)
    return volume_ref


@require_context
@_retry_on_deadlock
def volume_destroy(context, volume_id):
    session = get_session()
    updated_values = {'status': fields.VolumeStatus.DELETED,
                      'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at'),
                      'driver_data': None}
    with session.begin():
        volume_ref = _volume_get(context, volume_id, session=session)
        volume_ref.update(updated_values)
        volume_ref.save(session)
    del updated_values['updated_at']
    return updated_values


@require_context
def volume_get(context, volume_id, columns_to_join=[], read_deleted=None):
    return _volume_get(context, volume_id, columns_to_join=columns_to_join,
                       read_deleted=read_deleted)


@require_admin_context
def volume_get_all(context, marker, limit, sort_keys=None, sort_dirs=None,
                   filters=None, offset=None):
    session = get_session()
    with session.begin():
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


@require_context
def volume_get_all_by_project(context, project_id, marker, limit,
                              sort_keys=None, sort_dirs=None,
                              filters=None, offset=None):
    session = get_session()
    with session.begin():
        authorize_project_context(context, project_id)
        filters = filters.copy() if filters else {}
        filters['project_id'] = project_id
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


##############


@handle_db_data_error
@require_context
def replication_create(context, values):
    replication = models.Replication()
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    replication.update(values)
    session = get_session()
    with session.begin():
        replication.save(session)
        return replication


@handle_db_data_error
@require_context
def replication_update(context, replication_id, values):
    session = get_session()
    with session.begin():
        replication = model_query(context, models.Replication, session=session,
                                  read_deleted='yes'). \
            filter_by(id=replication_id).first()

        if not replication:
            raise exception.ReplicationNotFound(replication_id=replication_id)

        replication.update(values)

    return replication


@require_admin_context
@_retry_on_deadlock
def replication_destroy(context, replication_id):
    session = get_session()
    updated_values = {'status': fields.ReplicateStatus.DELETED,
                      'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at')}

    with session.begin():
        replication_ref = _replication_get(context, replication_id,
                                           session=session)
        replication_ref.update(updated_values)
        replication_ref.save(session)
    del updated_values['updated_at']
    return updated_values


def _replication_get(context, replication_id, session=None, project_only=True):
    result = model_query(context, models.Replication, session=session,
                         project_only=project_only). \
        filter_by(id=replication_id). \
        first()

    if not result:
        raise exception.ReplicationNotFound(replication_id=replication_id)

    return result


@require_context
def replication_get(context, replication_id, project_only=True):
    return _replication_get(context, replication_id, project_only=project_only)


def _replication_get_all(context, filters=None, marker=None, limit=None,
                         offset=None, sort_keys=None, sort_dirs=None):
    if filters and not is_valid_model_filters(models.Replication, filters):
        return []

    session = get_session()
    with session.begin():
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters,
                                         offset, models.Replication)
        if query is None:
            return []
        return query.all()


@require_admin_context
def replication_get_all(context, marker, limit, sort_keys=None, sort_dirs=None,
                        filters=None, offset=None):
    return _replication_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs)


@require_context
def replication_get_all_by_project(context, project_id, marker, limit,
                                   sort_keys=None, sort_dirs=None,
                                   filters=None, offset=None):
    authorize_project_context(context, project_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["project_id"] = project_id

    return _replication_get_all(context, filters, marker, limit, offset,
                                sort_keys, sort_dirs)


def _replications_get_query(context, session=None, project_only=False):
    return model_query(context, models.Replication, session=session,
                       project_only=project_only)


def _process_replications_filters(query, filters):
    if filters:
        if not is_valid_model_filters(models.Replication, filters):
            return
        query = query.filter_by(**filters)
    return query


###############


@handle_db_data_error
@require_context
def snapshot_create(context, values):
    snapshot = models.Snapshot()
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    snapshot.update(values)
    session = get_session()
    with session.begin():
        snapshot.save(session)
        return snapshot


@handle_db_data_error
@require_context
def snapshot_update(context, snapshot_id, values):
    session = get_session()
    with session.begin():
        snapshot = model_query(context, models.Snapshot, session=session,
                               read_deleted='yes'). \
            filter_by(id=snapshot_id).first()

        if not snapshot:
            raise exception.SnapshotNotFound(snapshot_id=snapshot_id)

        snapshot.update(values)

    return snapshot


@require_admin_context
@_retry_on_deadlock
def snapshot_destroy(context, snapshot_id):
    session = get_session()
    updated_values = {'status': fields.SnapshotStatus.DELETED,
                      'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at')}
    with session.begin():
        snapshot_ref = _snapshot_get(context, snapshot_id, session=session)
        snapshot_ref.update(updated_values)
        snapshot_ref.save(session)
    del updated_values['updated_at']
    return updated_values


def _snapshot_get(context, snapshot_id, session=None, project_only=True):
    result = model_query(context, models.Snapshot, session=session,
                         project_only=project_only). \
        filter_by(id=snapshot_id). \
        first()

    if not result:
        raise exception.SnapshotNotFound(snapshot_id=snapshot_id)

    return result


@require_context
def snapshot_get(context, snapshot_id, project_only=True):
    return _snapshot_get(context, snapshot_id, project_only=project_only)


def _snapshot_get_all(context, filters=None, marker=None, limit=None,
                      offset=None, sort_keys=None, sort_dirs=None):
    if filters and not is_valid_model_filters(models.Snapshot, filters):
        return []

    session = get_session()
    with session.begin():
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters,
                                         offset, models.Snapshot)
        if query is None:
            return []
        return query.all()


@require_admin_context
def snapshot_get_all(context, marker, limit, sort_keys=None, sort_dirs=None,
                     filters=None, offset=None):
    return _snapshot_get_all(context, filters, marker, limit, offset,
                             sort_keys, sort_dirs)


@require_context
def snapshot_get_all_by_project(context, project_id, marker, limit,
                                sort_keys=None, sort_dirs=None,
                                filters=None, offset=None):
    authorize_project_context(context, project_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["project_id"] = project_id

    return _snapshot_get_all(context, filters, marker, limit, offset,
                             sort_keys, sort_dirs)


@require_context
def snapshot_get_all_by_volume(context, volume_id, filters=None):
    # authorize_project_context(context, volume_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["volume_id"] = volume_id

    return _snapshot_get_all(context, filters)


@require_context
def snapshot_get_all_by_checkpoint(context, checkpoint_id, filters=None):
    # authorize_project_context(context, checkpoint_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["checkpoint_id"] = checkpoint_id

    return _snapshot_get_all(context, filters)


def _snapshots_get_query(context, session=None, project_only=False):
    return model_query(context, models.Snapshot, session=session,
                       project_only=project_only)


def _process_snapshots_filters(query, filters):
    if filters:
        if not is_valid_model_filters(models.Snapshot, filters):
            return
        query = query.filter_by(**filters)
    return query


###################


@handle_db_data_error
@require_context
def backup_create(context, values):
    backup = models.Backup()
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    backup.update(values)
    session = get_session()
    with session.begin():
        backup.save(session)
        return backup


@handle_db_data_error
@require_context
def backup_update(context, backup_id, values):
    session = get_session()
    with session.begin():
        backup = model_query(context, models.Backup, session=session,
                             read_deleted='yes'). \
            filter_by(id=backup_id).first()
        if not backup:
            raise exception.BackupNotFound(backup_id=backup_id)

        backup.update(values)

    return backup


@require_admin_context
@_retry_on_deadlock
def backup_destroy(context, backup_id):
    session = get_session()
    updated_values = {'status': fields.BackupStatus.DELETED,
                      'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at')}

    with session.begin():
        backup_ref = _backup_get(context, backup_id, session=session)
        backup_ref.update(updated_values)
        backup_ref.save(session)
    del updated_values['updated_at']
    return updated_values


@require_context
def backup_get(context, backup_id, project_only=True):
    return _backup_get(context, backup_id, project_only=project_only)


def _backup_get_all(context, filters=None, marker=None, limit=None,
                    offset=None, sort_keys=None, sort_dirs=None):
    if filters and not is_valid_model_filters(models.Backup, filters):
        return []

    session = get_session()
    with session.begin():
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters,
                                         offset, models.Backup)

        if query is None:
            return []
        return query.all()


@require_admin_context
def backup_get_all(context, marker, limit, sort_keys=None, sort_dirs=None,
                   filters=None, offset=None):
    return _backup_get_all(context, filters, marker, limit, offset, sort_keys,
                           sort_dirs)


@require_context
def backup_get_all_by_project(context, project_id, marker, limit,
                              sort_keys=None, sort_dirs=None,
                              filters=None, offset=None):
    authorize_project_context(context, project_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["project_id"] = project_id

    return _backup_get_all(context, filters, marker, limit, offset, sort_keys,
                           sort_dirs)


@require_context
def backup_get_all_by_volume(context, volume_id, filters=None):
    # authorize_project_context(context, volume_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["volume_id"] = volume_id

    return _backup_get_all(context, filters)


def _backups_get_query(context, session=None, project_only=False):
    return model_query(context, models.Backup, session=session,
                       project_only=project_only)


def _process_backups_filters(query, filters):
    if filters:
        if not is_valid_model_filters(models.Backup, filters):
            return
        query = query.filter_by(**filters)
    return query


def _backup_get(context, backup_id, session=None, project_only=True):
    result = model_query(context, models.Backup, session=session,
                         project_only=project_only). \
        filter_by(id=backup_id). \
        first()

    if not result:
        raise exception.BackupNotFound(backup_id=backup_id)

    return result


################


@require_admin_context
def volume_attach(context, values):
    volume_attachment_ref = models.VolumeAttachment()
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    volume_attachment_ref.update(values)
    session = get_session()
    with session.begin():
        volume_attachment_ref.save(session)
        return volume_attachment_get(context, values['id'], session=session)


@require_admin_context
def volume_attached(context, attachment_id, instance_uuid, host_name,
                    mountpoint, attach_mode='rw'):
    """This method updates a volume attachment entry.

        This function saves the information related to a particular
        attachment for a volume.  It also updates the volume record
        to mark the volume as attached.

        """
    if instance_uuid and not uuidutils.is_uuid_like(instance_uuid):
        raise exception.InvalidUUID(uuid=instance_uuid)

    session = get_session()
    with session.begin():
        volume_attachment_ref = volume_attachment_get(context, attachment_id,
                                                      session=session)

        volume_attachment_ref['mountpoint'] = mountpoint
        volume_attachment_ref['attach_status'] = 'attached'
        volume_attachment_ref['instance_uuid'] = instance_uuid
        volume_attachment_ref['attached_host'] = host_name
        volume_attachment_ref['attach_time'] = timeutils.utcnow()
        volume_attachment_ref['attach_mode'] = attach_mode

        volume_ref = _volume_get(context, volume_attachment_ref['volume_id'],
                                 session=session)
        volume_attachment_ref.save(session=session)

        volume_ref['status'] = 'in-use'
        volume_ref['attach_status'] = 'attached'
        volume_ref.save(session=session)
        return volume_ref


@require_context
def volume_attachment_get(context, attachment_id, session=None):
    result = model_query(context, models.VolumeAttachment,
                         session=session). \
        filter_by(id=attachment_id). \
        first()
    if not result:
        raise exception.VolumeAttachmentNotFound(filter='attachment_id = %s' %
                                                        attachment_id)
    return result


@require_context
def volume_attachment_get_all_by_volume_id(context, volume_id, session=None):
    result = model_query(context, models.VolumeAttachment,
                         session=session). \
        filter_by(volume_id=volume_id). \
        filter(models.VolumeAttachment.attach_status != 'detached'). \
        all()
    return result


@require_context
def volume_attachment_get_all_by_host(context, volume_id, host):
    session = get_session()
    with session.begin():
        result = model_query(context, models.VolumeAttachment,
                             session=session). \
            filter_by(volume_id=volume_id). \
            filter_by(attached_host=host). \
            filter(models.VolumeAttachment.attach_status != 'detached'). \
            all()
        return result


@require_context
def volume_attachment_get_all_by_instance_uuid(context, volume_id,
                                               instance_uuid):
    session = get_session()
    with session.begin():
        result = model_query(context, models.VolumeAttachment,
                             session=session). \
            filter_by(volume_id=volume_id). \
            filter_by(instance_uuid=instance_uuid). \
            filter(models.VolumeAttachment.attach_status != 'detached'). \
            all()
        return result


@require_context
def volume_attachment_update(context, attachment_id, values):
    session = get_session()
    with session.begin():
        volume_attachment_ref = volume_attachment_get(context, attachment_id,
                                                      session=session)
        volume_attachment_ref.update(values)
        volume_attachment_ref.save(session=session)
        return volume_attachment_ref


@require_admin_context
def volume_detached(context, volume_id, attachment_id):
    """This updates a volume attachment and marks it as detached.

        This method also ensures that the volume entry is correctly
        marked as either still attached/in-use or detached/available
        if this was the last detachment made.

        """
    session = get_session()
    with session.begin():
        attachment = None
        try:
            attachment = volume_attachment_get(context, attachment_id,
                                               session=session)
        except exception.VolumeAttachmentNotFound:
            pass

        # If this is already detached, attachment will be None
        if attachment:
            now = timeutils.utcnow()
            attachment['attach_status'] = 'detached'
            attachment['detach_time'] = now
            attachment['deleted'] = True
            attachment['deleted_at'] = now
            attachment.save(session=session)

        attachment_list = volume_attachment_get_all_by_volume_id(
            context, volume_id, session=session)
        remain_attachment = False
        if attachment_list and len(attachment_list) > 0:
            remain_attachment = True

        volume_ref = _volume_get(context, volume_id, session=session)
        if not remain_attachment:
            volume_ref['attach_status'] = 'detached'
            volume_ref.save(session=session)
        else:
            # Volume is still attached
            volume_ref['status'] = 'in-use'
            volume_ref['attach_status'] = 'attached'
            volume_ref.save(session=session)

@handle_db_data_error
@require_context
def attachment_destroy(context, attachment_id):
    """Destroy the specified attachment record."""
    utcnow = timeutils.utcnow()
    session = get_session()
    with session.begin():
        updated_values = {'attach_status': 'deleted',
                          'deleted': True,
                          'deleted_at': utcnow,
                          'updated_at': literal_column('updated_at')}
        volume_attachment_ref = volume_attachment_get(context, attachment_id,
                                                      session=session)
        volume_attachment_ref.update(updated_values)
    del updated_values['updated_at']
    return updated_values


###################

@handle_db_data_error
@require_context
def checkpoint_create(context, values):
    checkpoint = models.Checkpoint()
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())

    checkpoint.update(values)
    session = get_session()
    with session.begin():
        checkpoint.save(session)
        return checkpoint


@handle_db_data_error
@require_context
def checkpoint_update(context, checkpoint_id, values):
    session = get_session()
    with session.begin():
        checkpoint = model_query(context, models.Checkpoint, session=session,
                                 read_deleted='yes'). \
            filter_by(id=checkpoint_id).first()
        if not checkpoint:
            raise exception.CheckpointNotFound(checkpoint_id=checkpoint_id)

        checkpoint.update(values)

    return checkpoint


@require_admin_context
@_retry_on_deadlock
def checkpoint_destroy(context, checkpoint_id):
    session = get_session()
    updated_values = {'status': fields.CheckpointStatus.DELETED,
                      'deleted': True,
                      'deleted_at': timeutils.utcnow(),
                      'updated_at': literal_column('updated_at')}

    with session.begin():
        checkpoint_ref = _checkpoint_get(context, checkpoint_id,
                                         session=session)
        checkpoint_ref.update(updated_values)
        checkpoint_ref.save(session)
    del updated_values['updated_at']
    return updated_values


@require_context
def checkpoint_get(context, checkpoint_id, project_only=True):
    return _checkpoint_get(context, checkpoint_id, project_only=project_only)


def _checkpoint_get_all(context, filters=None, marker=None, limit=None,
                        offset=None, sort_keys=None, sort_dirs=None):
    if filters and not is_valid_model_filters(models.Checkpoint, filters):
        return []

    session = get_session()
    with session.begin():
        query = _generate_paginate_query(context, session, marker, limit,
                                         sort_keys, sort_dirs, filters,
                                         offset, models.Checkpoint)

        if query is None:
            return []
        return query.all()


@require_admin_context
def checkpoint_get_all(context, marker, limit, sort_keys=None, sort_dirs=None,
                       filters=None, offset=None):
    return _checkpoint_get_all(context, filters, marker, limit, offset,
                               sort_keys,
                               sort_dirs)


@require_context
def checkpoint_get_all_by_project(context, project_id, marker, limit,
                                  sort_keys=None, sort_dirs=None,
                                  filters=None, offset=None):
    authorize_project_context(context, project_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["project_id"] = project_id

    return _checkpoint_get_all(context, filters, marker, limit, offset,
                               sort_keys,
                               sort_dirs)


@require_context
def checkpoint_get_all_by_replication(context, replication_id, filters=None):
    # authorize_project_context(context, replication_id)
    if not filters:
        filters = {}
    else:
        filters = filters.copy()

    filters["replication_id"] = replication_id

    return _checkpoint_get_all(context, filters)


def _checkpoints_get_query(context, session=None, project_only=False):
    return model_query(context, models.Checkpoint, session=session,
                       project_only=project_only)


def _process_checkpoints_filters(query, filters):
    if filters:
        if not is_valid_model_filters(models.Checkpoint, filters):
            return
        query = query.filter_by(**filters)
    return query


def _checkpoint_get(context, checkpoint_id, session=None, project_only=True):
    result = model_query(context, models.Checkpoint, session=session,
                         project_only=project_only). \
        filter_by(id=checkpoint_id). \
        first()

    if not result:
        raise exception.CheckpointNotFound(checkpoint_id=checkpoint_id)

    return result


###################

PAGINATION_HELPERS = {
    models.Volume: (_volume_get_query,
                    _process_volume_filters,
                    _volume_get),
    models.Snapshot: (_snapshots_get_query,
                      _process_snapshots_filters,
                      _snapshot_get),
    models.Backup: (_backups_get_query,
                    _process_backups_filters,
                    _backup_get),
    models.Replication: (_replications_get_query,
                         _process_replications_filters,
                         _replication_get),
    models.Checkpoint: (_checkpoints_get_query,
                        _process_checkpoints_filters,
                        _checkpoint_get)
}


def _generate_paginate_query(context, session, marker, limit, sort_keys,
                             sort_dirs, filters, offset=None,
                             paginate_type=models.Volume, use_model=False,
                             **kwargs):
    """Generate the query to include the filters and the paginate options.

    Returns a query with sorting / pagination criteria added or None
    if the given filters will not yield any results.

    :param context: context to query under
    :param session: the session to use
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param limit: maximum number of items to return
    :param sort_keys: list of attributes by which results should be sorted,
                      paired with corresponding item in sort_dirs
    :param sort_dirs: list of directions in which results should be sorted,
                      paired with corresponding item in sort_keys
    :param filters: dictionary of filters; values that are in lists, tuples,
                    or sets cause an 'IN' operation, while exact matching
                    is used for other values, see _process_plan_filters
                    function for more information
    :param offset: number of items to skip
    :param paginate_type: type of pagination to generate
    :returns: updated query or None
    """
    get_query, process_filters, get = PAGINATION_HELPERS[paginate_type]

    sort_keys, sort_dirs = process_sort_params(sort_keys,
                                               sort_dirs,
                                               default_dir='desc')
    if use_model:
        query = get_query(context, session=session, **kwargs)
    else:
        query = get_query(context, session=session)

    if filters:
        query = process_filters(query, filters)
        if query is None:
            return None

    marker_object = None
    if marker is not None:
        marker_object = get(context, marker, session=session)

    return sqlalchemyutils.paginate_query(query, paginate_type, limit,
                                          sort_keys,
                                          marker=marker_object,
                                          sort_dirs=sort_dirs)


def process_sort_params(sort_keys, sort_dirs, default_keys=None,
                        default_dir='asc'):
    """Process the sort parameters to include default keys.

    Creates a list of sort keys and a list of sort directions. Adds the default
    keys to the end of the list if they are not already included.

    When adding the default keys to the sort keys list, the associated
    direction is:
    1) The first element in the 'sort_dirs' list (if specified), else
    2) 'default_dir' value (Note that 'asc' is the default value since this is
    the default in sqlalchemy.utils.paginate_query)

    :param sort_keys: List of sort keys to include in the processed list
    :param sort_dirs: List of sort directions to include in the processed list
    :param default_keys: List of sort keys that need to be included in the
                         processed list, they are added at the end of the list
                         if not already specified.
    :param default_dir: Sort direction associated with each of the default
                        keys that are not supplied, used when they are added
                        to the processed list
    :returns: list of sort keys, list of sort directions
    :raise exception.InvalidInput: If more sort directions than sort keys
                                   are specified or if an invalid sort
                                   direction is specified
    """
    if default_keys is None:
        default_keys = ['created_at', 'id']

    # Determine direction to use for when adding default keys
    if sort_dirs and len(sort_dirs):
        default_dir_value = sort_dirs[0]
    else:
        default_dir_value = default_dir

    # Create list of keys (do not modify the input list)
    if sort_keys:
        result_keys = list(sort_keys)
    else:
        result_keys = []

    # If a list of directions is not provided, use the default sort direction
    # for all provided keys.
    if sort_dirs:
        result_dirs = []
        # Verify sort direction
        for sort_dir in sort_dirs:
            if sort_dir not in ('asc', 'desc'):
                msg = _("Unknown sort direction, must be 'desc' or 'asc'.")
                raise exception.InvalidInput(reason=msg)
            result_dirs.append(sort_dir)
    else:
        result_dirs = [default_dir_value for _sort_key in result_keys]

    # Ensure that the key and direction length match
    while len(result_dirs) < len(result_keys):
        result_dirs.append(default_dir_value)
    # Unless more direction are specified, which is an error
    if len(result_dirs) > len(result_keys):
        msg = _("Sort direction array size exceeds sort key array size.")
        raise exception.InvalidInput(reason=msg)

    # Ensure defaults are included
    for key in default_keys:
        if key not in result_keys:
            result_keys.append(key)
            result_dirs.append(default_dir_value)

    return result_keys, result_dirs
