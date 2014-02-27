import functools
import random
import re, time
import eventlet
from eventlet import greenthread
import greenlet
from oslo.config import cfg

from cinder import exception
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils
from cinder import utils
from cinder.volume.drivers.san import SanISCSIDriver

LOG = logging.getLogger(__name__)

print 123

fjs_opts = [
    cfg.StrOpt('fjs_lun_group_name',
               default='Luntest-lg',
               help='Group name to use for creating volumes'),
    cfg.IntOpt('fjs_cli_timeout',
               default=30,
               help='Timeout for the Group Manager cli command execution'),
    cfg.IntOpt('fjs_cli_max_retries',
               default=2,
               help='Maximum retry count for reconnection'),
    cfg.BoolOpt('fjs_use_chap',
                default=False,
                help='Use CHAP authentificaion for targets?'),
#    cfg.StrOpt('fjs_chap_login',
#               default='root',
#               help='Existing CHAP account name'),
#    cfg.StrOpt('fjs_chap_password',
#               default='1a2b3c4d',
#               help='Password for specified CHAP account name',
#               secret=True)
    cfg.StrOpt('fjs_pool',
               default='Thin-01',
               help='Pool in which volumes will be created'),
    cfg.StrOpt('group_ip',
               default='10.160.0.72',
               help='IP of iscsi server, which listens on port 3260'),
]
CONF = cfg.CONF
CONF.register_opts(fjs_opts)

def with_timeout(f):
    @functools.wraps(f)
    def __inner(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', None)
        gt = eventlet.spawn(f, self, *args, **kwargs)
        if timeout is None:
            return gt.wait()
        else:
            kill_thread = eventlet.spawn_after(timeout, gt.kill)
            try:
                res = gt.wait()
            except greenlet.GreenletExit:
                raise exception.VolumeBackendAPIException(
                    data="Command timed out")
            else:
                kill_thread.cancel()
                return res

    return __inner


class FujitsuISCSIDriver(SanISCSIDriver):

    VERSION = "1.0.0"

    def __init__(self, *args, **kwargs):
        super(FujitsuISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(fjs_opts)
#        self._group_ip = 'clgt wtf'
        self.sshpool = None

    def _get_output(self, chan):
        out = ''
        ending = 'CLI> '
        while not out.endswith(ending):
            out += chan.recv(1024000)

        LOG.debug(_("CLI output\n%s"), out)
        return out.splitlines()

    def _get_prefixed_value(self, lines, prefix):
        for line in lines:
            if line.startswith(prefix):
                return line[len(prefix):]
        return

#    @with_timeout   
    def _ssh_execute(self, ssh, command, *arg, **kwargs):
        chan = ssh.invoke_shell()

        LOG.debug(_("Reading CLI MOTD"))
        self._get_output(chan)

        cmd = 'stty columns 255'
        LOG.debug(_("Setting CLI terminal width: '%s'"), cmd)
        chan.send(cmd + '\r')
        out = self._get_output(chan)

        LOG.debug(_("Sending CLI command: '%s'"), command)
        chan.send(command + '\r')
        out = self._get_output(chan)
        chan.close()
        if any(line.startswith(('% Error', 'Error:')) for line in out):
            desc = _("Error executing FJS command")
            cmdout = '\n'.join(out)
            LOG.error(cmdout)
            raise processutils.ProcessExecutionError(
                stdout=cmdout, cmd=command, description=desc)
        return out

    def _run_ssh(self, cmd_list, attempts=1):
#	utils.check_ssh_injection(cmd_list)
        command = ' '. join(cmd_list)

        if not self.sshpool:
            password = self.configuration.san_password
            privatekey = self.configuration.san_private_key
            min_size = self.configuration.ssh_min_pool_conn
            max_size = self.configuration.ssh_max_pool_conn
            self.sshpool = utils.SSHPool(self.configuration.san_ip,
                                         self.configuration.san_ssh_port,
                                         self.configuration.ssh_conn_timeout,
                                         self.configuration.san_login,
                                         password=password,
                                         privatekey=privatekey,
                                         min_size=min_size,
                                         max_size=max_size)
        try:
            total_attempts = attempts
            with self.sshpool.item() as ssh:
                while attempts > 0:
                    attempts -= 1
                    try:
                        LOG.info(_('FJS-driver: executing "%s"') % command)
                        return self._ssh_execute(
                            ssh, command,
                            timeout=self.configuration.fjs_cli_timeout)
                    except processutils.ProcessExecutionError:
                        raise
                    except Exception as e:
                        LOG.exception(e)
                        greenthread.sleep(random.randint(20, 500) / 100.0)
                msg = (_("SSH Command failed after '%(total_attempts)r' "
                         "attempts : '%(command)s'") %
                       {'total_attempts': total_attempts, 'command': command})
                raise exception.VolumeBackendAPIException(data=msg)

        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Error running SSH command: %s") % command) 

    def _fjs_execute(self, *args, **kwargs):
        return self._run_ssh(
            args, attempts=self.configuration.fjs_cli_max_retries) 

###############################################################################


#    def _get_volume_data(self, lines):
#        prefix = 'iSCSI target name is '
#        target_name = self._get_prefixed_value(lines, prefix)[:-1]
#        lun_id = "%s:%s,1 %s 0" % (self.configuration._group_ip, '3260', target_name)
#        model_update = {}
#        model_update['provider_location'] = lun_id
#        if self.configuration.fjs_use_chap:
#            model_update['provider_auth'] = 'CHAP %s %s' % \
#                (self.configuration.fjs_chap_login,
#                 self.configuration.fjs_chap_password)
#        return model_update

    def _name_translate(self, name):
        """Form new names for volume and snapshot because of
        32-character limit on names.
        """
        newname = 'vol_' + str(hash(name))[-10:]
#        newname = name
        LOG.debug(_('_name_translate: Name in cinder: %(old)s, new name in '
                    'storage system: %(new)s') % {'old': name, 'new': newname})

        return newname

    def _show_lun_group_info(self, lun_group_name):
        cmd = ['show', 'lun-groups', '-lg-name', lun_group_name]
        try:
           out = self._fjs_execute(*cmd)
           return out[5:-1]
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to create volume because cannot show lun group information'))
 
    def _map_volume_to_lun_group(self, volume_name, lun_group_name):

        lun_group_info = self._show_lun_group_info(lun_group_name)
        lun_id_list = [re.search(r"(\d+)", line).group() for line in lun_group_info]
        lun_id = -1 

        for i in range(256):
            if str(i) not in lun_id_list:
                lun_id = str(i)
                break

        if lun_id == -1:
            msg = (_("This lun group is full (256 lun ids)"))
            raise exception.VolumeBackendAPIException(data=msg)

        cmd = ['set', 'lun-group', '-lg-name', lun_group_name ,'-volume-name', volume_name ,'-lun', lun_id]
        try:
           time.sleep(1)
           out = self._fjs_execute(*cmd)
           target_name = 'iqn.1991-05.com.nhapq:123456'
           data_iscsi = "%s:%s,1 %s" % (self.configuration.group_ip, '3260', target_name)
           model_update = {}
           model_update['provider_location'] = data_iscsi
           if self.configuration.fjs_use_chap:
                model_update['provider_auth'] = 'CHAP %s %s' % \
                       (self.configuration.fjs_chap_login,
                        self.configuration.fjs_chap_password)
           return model_update
           
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to map volume  %s to lun group %s' % (volume_name, lun_group_name)))

#################

    def _get_space_in_gb(self, val):
        scale = 1.0
        part = 'GB'
        if val.endswith('MB'):
            scale = 1.0 / 1024
            part = 'MB'
        elif val.endswith('TB'):
            scale = 1.0 * 1024
            part = 'TB'
        result =  scale * float(val.partition(part)[0])
        return "{0:.2f}".format(result)
###################


    def _update_volume_stats(self):
        """Retrieve stats info from eqlx group."""

        LOG.debug(_("Updating volume stats"))
        data = {}
        backend_name = "fjs"
        if self.configuration:
            backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = backend_name or 'fjs'
        data["vendor_name"] = 'Fujitsu'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'iSCSI'

        data['reserved_percentage'] = 0
        data['QoS_support'] = False

        data['total_capacity_gb'] = 'infinite'
        data['free_capacity_gb'] = 'infinite'
        result = self._fjs_execute('show','thin-pro-pools')
        import re
        result = re.search(r'((?P<total_size>\d+\.\d+ [GKTM]B))\s+(?P<used_size>\d+\.\d+ [GKTM]B)', result[3])

        data['total_capacity_gb'] = self._get_space_in_gb(result.group('total_size'))
        data['used_capacity_gb'] = self._get_space_in_gb(result.group('used_size'))
        data['free_capacity_gb'] = float(data['total_capacity_gb']) - float(data['used_capacity_gb'])
        
        self._stats = data


    def _check_volume(self, volume_name):
        """Check if the volume exists on the Array."""
        try:
            cmd = ['show', 'volumes']
            result = self._fjs_execute(*cmd)
            if volume_name in " ".join(result):
                return True
            else: 
                LOG.error(_('Volume %s does not exist, it may have already been deleted'), volume_name)
                raise exception.VolumeNotFound(volume_id=volume_name)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to find volume %s'), volume_name)


    def create_volume(self, volume):
        """Create a volume, then map volume to an available lun id (from 0 to 255) and return lun id"""
        try:
            volume_name = self._name_translate(volume['name'])
            lun_group_name = self.configuration.fjs_lun_group_name
            cmd = ['Create', 'volume','-name', volume_name, '-size', "%sgb" % (volume['size']), '-pool-name', self.configuration.fjs_pool, '-type', 'tpv']
        #    if self.configuration.fjs_pool != 'default':
        #        cmd.append('-pool-name')
        #        cmd.append(self.configuration.fjs_pool)
        #    if self.configuration.san_thin_provision:
        #        cmd.extend(['-type', 'tpv'])
            out = self._fjs_execute(*cmd)
            # fix race effect
            time.sleep(1)
            return self._map_volume_to_lun_group(volume_name, lun_group_name)
            raise
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to create volume %s'), volume_name)

    def delete_volume(self, volume):
        """Delete a volume: delete the mapping from volume to lun id, then remove the volume"""
        try:
            volume_name = self._name_translate(volume['name'])
            self._check_volume(volume_name)
            time.sleep(1)
            #find lun_group_name and lun id associated with this volume
           
            mapping_info = self._fjs_execute('show', 'volume-mapping', '-volume-name', volume_name)
            infor = re.search(r'(?P<lun_id>\d+)\s+\d+\s+(?P<lun_group_name>[\S]*)', str(mapping_info[-2:-1]))
            lun_id = infor.group('lun_id')
            lun_group_name = infor.group('lun_group_name')
           
            time.sleep(1)
            self._fjs_execute('delete', 'lun-group', '-lg-name', lun_group_name, '-lun', lun_id)
            time.sleep(1)

            self._fjs_execute('delete', 'volume', '-volume-name', volume_name)
            return "lund_is : %s, lun_group_name: %s" %(lun_id, lun_group_name)
        except exception.VolumeNotFound:
            LOG.warn(_('Volume %s was not found while trying to delete it'), volume_name)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to delete volume %s'), volume_name) 


    def extend_volume(self, volume, new_size):
        """Extend the size of the volume."""
        try:
            volume_name = self._name_translate(volume['name'])
            new_size = int(float(new_size))
            self._fjs_execute('expand', 'volume', '-volume-name', volume_name,
                              '-size', "%sgb" % new_size)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_('Failed to extend_volume %(name)s from '
                            '%(current_size)sGB to %(new_size)sGB'),
                          {'name': volume_name,
                           'current_size': volume['size'],
                           'new_size': new_size})


