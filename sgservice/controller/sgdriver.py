# Copyright 2012, Red Hat, Inc.
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

import abc
import six

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


@six.add_metaclass(abc.ABCMeta)
class SGDriver(object):
    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def enable_sg(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def disable_sg(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_volume(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def list_volumes(self):
        pass

    @abc.abstractmethod
    def attach_volume(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def detach_volume(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def initialize_connection(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def create_backup(self, **kwargs):
        pass

    @abc.abstractmethod
    def delete_backup(self, **kwargs):
        pass

    @abc.abstractmethod
    def restore_backup(self, **kwargs):
        pass

    @abc.abstractmethod
    def create_snapshot(self, **kwargs):
        pass

    @abc.abstractmethod
    def delete_snapshot(self, **kwargs):
        pass

    @abc.abstractmethod
    def enable_replicate(self, **kwargs):
        pass

    @abc.abstractmethod
    def disable_replicate(self, **kwarg):
        pass

    @abc.abstractmethod
    def failover_replicate(self, **kwargs):
        pass

    @abc.abstractmethod
    def delete_replicate(self, **kwargs):
        pass

    @abc.abstractmethod
    def reverse_replicate(self, **kwargs):
        pass
