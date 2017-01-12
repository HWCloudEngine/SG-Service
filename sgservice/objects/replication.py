#    Copyright 2015 Intel Corporation
#
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

from oslo_config import cfg
from oslo_versionedobjects import fields

from sgservice import db
from sgservice import exception
from sgservice import objects
from sgservice.objects import base
from sgservice.objects import fields as s_fields

CONF = cfg.CONF


@base.SGServiceObjectRegistry.register
class Replication(base.SGServicePersistentObject, base.SGServiceObject,
                  base.SGServiceObjectDictCompat):
    VERSION = '1.0'

    fields = {
        'id': fields.UUIDField(),
        'host': fields.StringField(),
        'user_id': fields.StringField(),
        'project_id': fields.StringField(),
        'status': s_fields.ReplicateStatusField(nullable=True),
        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),

        'master_volume': fields.UUIDField(nullable=True),
        'slave_volume': fields.UUIDField(nullable=True)
    }

    # obj_extra_fields is used to hold properties that are not
    # usually part of the model
    obj_extra_fields = ['name']

    @property
    def name(self):
        return CONF.replication_name_template % self.id

    @staticmethod
    def _from_db_object(context, replication, db_replication):
        for name, field in replication.fields.items():
            value = db_replication.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            replication[name] = value

        replication._context = context
        replication.obj_reset_changes()
        return replication

    @base.remotable
    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.sgservice_obj_get_changes()

        db_replication = db.replication_create(self._context, updates)
        self._from_db_object(self._context, self, db_replication)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            db.replication_update(self._context, self.id, updates)
        self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.replication_destroy(self._context, self.id)
            self.update(updated_values)
            self.obj_reset_changes(updated_values.keys())


@base.SGServiceObjectRegistry.register
class ReplicationList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Replication'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        replications = db.replication_get_all(
            context, filters=filters, marker=marker, limit=limit,
            offset=offset, sort_keys=sort_keys, sort_dirs=sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Replication,
                                  replications)

    @classmethod
    def get_all_by_project(cls, context, project_id, filters=None,
                           marker=None, limit=None, offset=None,
                           sort_keys=None, sort_dirs=None):
        replications = db.replication_get_all_by_project(
            context, project_id=project_id, filters=filters, marker=marker,
            limit=limit, offset=offset, sort_keys=sort_keys,
            sort_dirs=sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Replication,
                                  replications)
