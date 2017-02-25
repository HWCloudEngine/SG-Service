#    Copyright 2015 Intel Corp.
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

from oslo_versionedobjects import fields

from sgservice import db
from sgservice import exception
from sgservice.i18n import _
from sgservice import objects
from sgservice.objects import base
from sgservice import utils


@base.SGServiceObjectRegistry.register
class Service(base.SGServicePersistentObject, base.SGServiceObject,
              base.SGServiceObjectDictCompat, base.SGServiceComparableObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': fields.IntegerField(),
        'host': fields.StringField(nullable=True),
        'binary': fields.StringField(nullable=True),
        'topic': fields.StringField(nullable=True),
        'report_count': fields.IntegerField(default=0),
        'disabled': fields.BooleanField(default=False, nullable=True),
        'disabled_reason': fields.StringField(nullable=True),
        'availability_zone': fields.StringField(nullable=True),
        'modified_at': fields.DateTimeField(nullable=True),
        'rpc_current_version': fields.StringField(nullable=True),
        'object_current_version': fields.StringField(nullable=True),
    }

    @staticmethod
    def _from_db_object(context, service, db_service):
        for name, field in service.fields.items():
            value = db_service.get(name)
            if isinstance(field, fields.IntegerField):
                value = value or 0
            elif isinstance(field, fields.DateTimeField):
                value = value or None
            service[name] = value

        service._context = context
        service.obj_reset_changes()
        return service

    @base.remotable_classmethod
    def get_by_host_and_topic(cls, context, host, topic):
        db_service = db.service_get_by_host_and_topic(context, host=host,
                                                      topic=topic)
        return cls._from_db_object(context, cls(context), db_service)

    @base.remotable_classmethod
    def get_by_args(cls, context, host, binary_key):
        db_service = db.service_get_by_args(context, host=host,
                                            binary=binary_key)
        return cls._from_db_object(context, cls(context), db_service)

    @base.remotable
    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason=_('already created'))
        updates = self.sgservice_obj_get_changes()
        db_service = db.service_create(self._context, updates)
        self._from_db_object(self._context, self, db_service)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            db.service_update(self._context, self.id, updates)
            self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.service_destroy(self._context, self.id)
            self.update(updated_values)
            self.obj_reset_changes(updated_values.keys())

    @property
    def is_up(self):
        """Check whether a service is up based on last heartbeat."""
        return utils.service_is_up(self)


@base.SGServiceObjectRegistry.register
class ServiceList(base.ObjectListBase, base.SGServiceObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Service'),
    }

    @classmethod
    def get_all(cls, context, filters=None):
        services = db.service_get_all(context, filters)
        return base.obj_make_list(context, cls(context), objects.Service,
                                  services)

    @classmethod
    def get_all_by_topic(cls, context, topic, disabled=None):
        services = db.service_get_all_by_topic(context, topic=topic,
                                               disabled=disabled)
        return base.obj_make_list(context, cls(context), objects.Service,
                                  services)
