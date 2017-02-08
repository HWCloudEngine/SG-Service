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

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class SGDriver(object):
    def __init__(self):
        pass

    def enable_sg(self, *args, **kwargs):
        pass

    def disable_sg(self, *args, **kwargs):
        pass

    def get_volume(self, *args, **kwargs):
        pass

    def list_volumes(self):
        pass

    def attach_volume(self, *args, **kwargs):
        pass

    def detach_volume(self, *args, **kwargs):
        pass

    def initialize_connection(self, *args, **kwargs):
        pass

    def create_backup(self, **kwargs):
        pass

    def delete_backup(self, **kwargs):
        pass

    def restore_backup(self, **kwargs):
        pass

    def create_snapshot(self, **kwargs):
        pass

    def delete_snapshot(self, **kwargs):
        pass

    def enable_replicate(self, **kwargs):
        pass

    def disable_replicate(self, **kwarg):
        pass

    def failover_replicate(self, **kwargs):
        pass

    def delete_replicate(self, **kwargs):
        pass

    def reverse_replicate(self, **kwargs):
        pass
