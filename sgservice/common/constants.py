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


# replicate_mode
REPLICATE_MODES = (REP_MASTER, REP_SLAVE) = ('master', 'slave')
SUPPORT_REPLICATE_MODES = [REP_MASTER, REP_SLAVE]

# access_modes
ACCESS_MODES = (ACCESS_RW, ACCESS_RO) = ('rw', 'ro')
SUPPORT_ACCESS_MODES = [ACCESS_RW, ACCESS_RO]

# backup type
BACKUP_TYPES = (FULL_BACKUP, INCREMENTAL_BACKUP) = ('full', 'incremental')
SUPPORT_BACKUP_TYPES = [FULL_BACKUP, INCREMENTAL_BACKUP]

# backup destinations
BACKUP_DESTINATIONS = (LOCAL_BACKUP, REMOTE_BACKUP) = ('local', 'remote')
SUPPORT_BACKUP_DESTINATIONS = [LOCAL_BACKUP, REMOTE_BACKUP]

# snapshot destinations
SNAPSHOT_DESTINATIONS = (LOCAL_SNAPSHOT, REMOTE_SNAPSHOT) = ('local', 'remote')
SUPPORT_SNAPSHOT_DESTINATIONS = [LOCAL_SNAPSHOT, REMOTE_SNAPSHOT]
