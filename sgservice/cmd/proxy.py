#!/usr/bin/env python
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

"""Starter script for karbor protection service."""

import eventlet
eventlet.monkey_patch()

import sys

from oslo_config import cfg
from oslo_log import log as logging

from sgservice import i18n
i18n.enable_lazy()
from sgservice import objects

# Need to register global_opts
from sgservice.common import config  # noqa
from sgservice import service
from sgservice import version


CONF = cfg.CONF


def main():
    objects.register_all()
    CONF(sys.argv[1:], project='sgservice',
         version=version.version_string())
    logging.setup(CONF, "sgservice")
    server = service.Service.create(binary='sgservice-controller')
    service.serve(server)
    service.wait()
