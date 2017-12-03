# coding=utf-8
# Author: Nic Wolfe <nic@wolfeden.ca>

#
# This file is part of Medusa.
#
# Medusa is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Medusa is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Medusa. If not, see <http://www.gnu.org/licenses/>.

import datetime
import logging
import os.path
import re

from contextlib2 import suppress
from medusa import app, common, helpers, logger, scheduler
from medusa.helper.common import try_int
from medusa.helpers.utils import split_and_strip
from medusa.logger.adapters.style import BraceAdapter
from medusa.version_checker import CheckVersion
from requests.compat import urlsplit
from six import iteritems, string_types, text_type
from six.moves.urllib.parse import urlunsplit, uses_netloc

log = BraceAdapter(logging.getLogger(__name__))
log.logger.addHandler(logging.NullHandler())

# Address poor support for scgi over unix domain sockets
# this is not nicely handled by python currently
# http://bugs.python.org/issue23636
uses_netloc.append('scgi')

naming_ep_type = ('%(seasonnumber)dx%(episodenumber)02d',
                  's%(seasonnumber)02de%(episodenumber)02d',
                  'S%(seasonnumber)02dE%(episodenumber)02d',
                  '%(seasonnumber)02dx%(episodenumber)02d')

sports_ep_type = ('%(seasonnumber)dx%(episodenumber)02d',
                  's%(seasonnumber)02de%(episodenumber)02d',
                  'S%(seasonnumber)02dE%(episodenumber)02d',
                  '%(seasonnumber)02dx%(episodenumber)02d')

naming_ep_type_text = ('1x02', 's01e02', 'S01E02', '01x02')

naming_multi_ep_type = {0: ['-%(episodenumber)02d'] * len(naming_ep_type),
                        1: [' - ' + x for x in naming_ep_type],
                        2: [x + '%(episodenumber)02d' for x in ('x', 'e', 'E', 'x')]}
naming_multi_ep_type_text = ('extend', 'duplicate', 'repeat')

naming_sep_type = (' - ', ' ')
naming_sep_type_text = (' - ', 'space')


def change_HTTPS_CERT(https_cert):
    """
    Replace HTTPS Certificate file path

    :param https_cert: path to the new certificate file
    :return: True on success, False on failure
    """
    if https_cert == '':
        app.HTTPS_CERT = ''
        return True

    if os.path.normpath(app.HTTPS_CERT) != os.path.normpath(https_cert):
        if helpers.make_dir(os.path.dirname(os.path.abspath(https_cert))):
            app.HTTPS_CERT = os.path.normpath(https_cert)
            log.info(u'Changed https cert path to {cert_path}', {u'cert_path': https_cert})
        else:
            return False

    return True


def change_HTTPS_KEY(https_key):
    """
    Replace HTTPS Key file path

    :param https_key: path to the new key file
    :return: True on success, False on failure
    """
    if https_key == '':
        app.HTTPS_KEY = ''
        return True

    if os.path.normpath(app.HTTPS_KEY) != os.path.normpath(https_key):
        if helpers.make_dir(os.path.dirname(os.path.abspath(https_key))):
            app.HTTPS_KEY = os.path.normpath(https_key)
            log.info(u'Changed https key path to {key_path}', {u'key_path': https_key})
        else:
            return False

    return True


def change_LOG_DIR(log_dir):
    """
    Change logging directory for application and webserver

    :param log_dir: Path to new logging directory
    :return: True on success, False on failure
    """
    abs_log_dir = os.path.normpath(os.path.join(app.DATA_DIR, log_dir))

    if os.path.normpath(app.LOG_DIR) != abs_log_dir:
        if not helpers.make_dir(abs_log_dir):
            return False

        app.ACTUAL_LOG_DIR = os.path.normpath(log_dir)
        app.LOG_DIR = abs_log_dir

    return True


def change_NZB_DIR(nzb_dir):
    """
    Change NZB Folder

    :param nzb_dir: New NZB Folder location
    :return: True on success, False on failure
    """
    if nzb_dir == '':
        app.NZB_DIR = ''
        return True

    if os.path.normpath(app.NZB_DIR) != os.path.normpath(nzb_dir):
        if helpers.make_dir(nzb_dir):
            app.NZB_DIR = os.path.normpath(nzb_dir)
            log.info(u'Changed NZB folder to {nzb_dir}', {'nzb_dir': nzb_dir})
        else:
            return False

    return True


def change_TORRENT_DIR(torrent_dir):
    """
    Change torrent directory

    :param torrent_dir: New torrent directory
    :return: True on success, False on failure
    """
    if torrent_dir == '':
        app.TORRENT_DIR = ''
        return True

    if os.path.normpath(app.TORRENT_DIR) != os.path.normpath(torrent_dir):
        if helpers.make_dir(torrent_dir):
            app.TORRENT_DIR = os.path.normpath(torrent_dir)
            log.info(u'Changed torrent folder to {torrent_dir}', {u'torrent_dir': torrent_dir})
        else:
            return False

    return True


def change_TV_DOWNLOAD_DIR(tv_download_dir):
    """
    Change TV_DOWNLOAD directory (used by postprocessor)

    :param tv_download_dir: New tv download directory
    :return: True on success, False on failure
    """
    if tv_download_dir == '':
        app.TV_DOWNLOAD_DIR = ''
        return True

    if os.path.normpath(app.TV_DOWNLOAD_DIR) != os.path.normpath(tv_download_dir):
        if helpers.make_dir(tv_download_dir):
            app.TV_DOWNLOAD_DIR = os.path.normpath(tv_download_dir)
            log.info(u'Changed TV download folder to {tv_download_dir}', {u'tv_download_dir': tv_download_dir})
        else:
            return False

    return True


def change_AUTOPOSTPROCESSOR_FREQUENCY(freq):
    """
    Change frequency of automatic postprocessing thread
    TODO: Make all thread frequency changers in config.py return True/False status

    :param freq: New frequency
    """
    app.AUTOPOSTPROCESSOR_FREQUENCY = try_int(freq, app.DEFAULT_AUTOPOSTPROCESSOR_FREQUENCY)

    if app.AUTOPOSTPROCESSOR_FREQUENCY < app.MIN_AUTOPOSTPROCESSOR_FREQUENCY:
        app.AUTOPOSTPROCESSOR_FREQUENCY = app.MIN_AUTOPOSTPROCESSOR_FREQUENCY

    app.auto_post_processor_scheduler.cycleTime = datetime.timedelta(minutes=app.AUTOPOSTPROCESSOR_FREQUENCY)


def change_TORRENT_CHECKER_FREQUENCY(freq):
    """
    Change frequency of Torrent Checker thread

    :param freq: New frequency
    """
    app.TORRENT_CHECKER_FREQUECY = try_int(freq, app.DEFAULT_TORRENT_CHECKER_FREQUENCY)

    if app.TORRENT_CHECKER_FREQUECY < app.MIN_TORRENT_CHECKER_FREQUENCY:
        app.TORRENT_CHECKER_FREQUECY = app.MIN_TORRENT_CHECKER_FREQUENCY

    app.torrent_checker_scheduler.cycleTime = datetime.timedelta(minutes=app.TORRENT_CHECKER_FREQUECY)


def change_DAILYSEARCH_FREQUENCY(freq):
    """
    Change frequency of daily search thread

    :param freq: New frequency
    """
    app.DAILYSEARCH_FREQUENCY = try_int(freq, app.DEFAULT_DAILYSEARCH_FREQUENCY)

    if app.DAILYSEARCH_FREQUENCY < app.MIN_DAILYSEARCH_FREQUENCY:
        app.DAILYSEARCH_FREQUENCY = app.MIN_DAILYSEARCH_FREQUENCY

    app.daily_search_scheduler.cycleTime = datetime.timedelta(minutes=app.DAILYSEARCH_FREQUENCY)


def change_BACKLOG_FREQUENCY(freq):
    """
    Change frequency of backlog thread

    :param freq: New frequency
    """
    app.BACKLOG_FREQUENCY = try_int(freq, app.DEFAULT_BACKLOG_FREQUENCY)

    app.MIN_BACKLOG_FREQUENCY = app.instance.get_backlog_cycle_time()
    if app.BACKLOG_FREQUENCY < app.MIN_BACKLOG_FREQUENCY:
        app.BACKLOG_FREQUENCY = app.MIN_BACKLOG_FREQUENCY

    app.backlog_search_scheduler.cycleTime = datetime.timedelta(minutes=app.BACKLOG_FREQUENCY)


def change_PROPERS_FREQUENCY(check_propers_interval):
    """
    Change frequency of backlog thread

    :param freq: New frequency
    """
    if not app.DOWNLOAD_PROPERS:
        return

    if app.CHECK_PROPERS_INTERVAL == check_propers_interval:
        return

    if check_propers_interval in app.PROPERS_SEARCH_INTERVAL:
        update_interval = datetime.timedelta(minutes=app.PROPERS_SEARCH_INTERVAL[check_propers_interval])
    else:
        update_interval = datetime.timedelta(hours=1)
    app.CHECK_PROPERS_INTERVAL = check_propers_interval
    app.proper_finder_scheduler.cycleTime = update_interval


def change_UPDATE_FREQUENCY(freq):
    """
    Change frequency of daily updater thread

    :param freq: New frequency
    """
    app.UPDATE_FREQUENCY = try_int(freq, app.DEFAULT_UPDATE_FREQUENCY)

    if app.UPDATE_FREQUENCY < app.MIN_UPDATE_FREQUENCY:
        app.UPDATE_FREQUENCY = app.MIN_UPDATE_FREQUENCY

    app.version_check_scheduler.cycleTime = datetime.timedelta(hours=app.UPDATE_FREQUENCY)


def change_SHOWUPDATE_HOUR(freq):
    """
    Change frequency of show updater thread

    :param freq: New frequency
    """
    app.SHOWUPDATE_HOUR = try_int(freq, app.DEFAULT_SHOWUPDATE_HOUR)

    if app.SHOWUPDATE_HOUR > 23:
        app.SHOWUPDATE_HOUR = 0
    elif app.SHOWUPDATE_HOUR < 0:
        app.SHOWUPDATE_HOUR = 0

    app.show_update_scheduler.start_time = datetime.time(hour=app.SHOWUPDATE_HOUR)


def change_SUBTITLES_FINDER_FREQUENCY(subtitles_finder_frequency):
    """
    Change frequency of subtitle thread

    :param subtitles_finder_frequency: New frequency
    """
    if subtitles_finder_frequency == '' or subtitles_finder_frequency is None:
        subtitles_finder_frequency = 1

    app.SUBTITLES_FINDER_FREQUENCY = try_int(subtitles_finder_frequency, 1)


def change_VERSION_NOTIFY(version_notify):
    """
    Change frequency of versioncheck thread

    :param version_notify: New frequency
    """

    oldSetting = app.VERSION_NOTIFY

    app.VERSION_NOTIFY = version_notify

    if not version_notify:
        app.NEWEST_VERSION_STRING = None

    if oldSetting is False and version_notify is True:
        app.version_check_scheduler.forceRun()


def change_GIT_PATH():
    """
    Recreate the version_check scheduler when GIT_PATH is changed.
    Force a run to clear or set any error messages.
    """
    app.version_check_scheduler = None
    app.version_check_scheduler = scheduler.Scheduler(
        CheckVersion(), cycleTime=datetime.timedelta(hours=app.UPDATE_FREQUENCY), threadName="CHECKVERSION", silent=False)
    app.version_check_scheduler.enable = True
    app.version_check_scheduler.start()
    app.version_check_scheduler.forceRun()


def change_DOWNLOAD_PROPERS(download_propers):
    """
    Enable/Disable proper download thread
    TODO: Make this return True/False on success/failure

    :param download_propers: New desired state
    """
    download_propers = checkbox_to_value(download_propers)

    if app.DOWNLOAD_PROPERS == download_propers:
        return

    app.DOWNLOAD_PROPERS = download_propers
    if app.DOWNLOAD_PROPERS:
        if not app.proper_finder_scheduler.enable:
            log.info(u'Starting PROPERFINDER thread')
            app.proper_finder_scheduler.silent = False
            app.proper_finder_scheduler.enable = True
        else:
            log.info(u'Unable to start PROPERFINDER thread. Already running')
    else:
        app.proper_finder_scheduler.enable = False
        app.trakt_checker_scheduler.silent = True
        log.info(u'Stopping PROPERFINDER thread')


def change_USE_TRAKT(use_trakt):
    """
    Enable/disable trakt thread
    TODO: Make this return true/false on success/failure

    :param use_trakt: New desired state
    """
    use_trakt = checkbox_to_value(use_trakt)

    if app.USE_TRAKT == use_trakt:
        return

    app.USE_TRAKT = use_trakt
    if app.USE_TRAKT:
        if not app.trakt_checker_scheduler.enable:
            log.info(u'Starting TRAKTCHECKER thread')
            app.trakt_checker_scheduler.silent = False
            app.trakt_checker_scheduler.enable = True
        else:
            log.info(u'Unable to start TRAKTCHECKER thread. Already running')
    else:
        app.trakt_checker_scheduler.enable = False
        app.trakt_checker_scheduler.silent = True
        log.info(u'Stopping TRAKTCHECKER thread')


def change_USE_SUBTITLES(use_subtitles):
    """
    Enable/Disable subtitle searcher
    TODO: Make this return true/false on success/failure

    :param use_subtitles: New desired state
    """
    use_subtitles = checkbox_to_value(use_subtitles)

    if app.USE_SUBTITLES == use_subtitles:
        return

    app.USE_SUBTITLES = use_subtitles
    if app.USE_SUBTITLES:
        if not app.subtitles_finder_scheduler.enable:
            log.info(u'Starting SUBTITLESFINDER thread')
            app.subtitles_finder_scheduler.silent = False
            app.subtitles_finder_scheduler.enable = True
        else:
            log.info(u'Unable to start SUBTITLESFINDER thread. Already running')
    else:
        app.subtitles_finder_scheduler.enable = False
        app.subtitles_finder_scheduler.silent = True
        log.info(u'Stopping SUBTITLESFINDER thread')


def change_PROCESS_AUTOMATICALLY(process_automatically):
    """
    Enable/Disable postprocessor thread
    TODO: Make this return True/False on success/failure

    :param process_automatically: New desired state
    """
    process_automatically = checkbox_to_value(process_automatically)

    if app.PROCESS_AUTOMATICALLY == process_automatically:
        return

    app.PROCESS_AUTOMATICALLY = process_automatically
    if app.PROCESS_AUTOMATICALLY:
        if not app.auto_post_processor_scheduler.enable:
            log.info(u'Starting POSTPROCESSOR thread')
            app.auto_post_processor_scheduler.silent = False
            app.auto_post_processor_scheduler.enable = True
        else:
            log.info(u'Unable to start POSTPROCESSOR thread. Already running')
    else:
        log.info(u'Stopping POSTPROCESSOR thread')
        app.auto_post_processor_scheduler.enable = False
        app.auto_post_processor_scheduler.silent = True


def CheckSection(CFG, sec):
    """ Check if INI section exists, if not create it """

    if sec in CFG:
        return True

    CFG[sec] = {}
    return False


def checkbox_to_value(option, value_on=1, value_off=0):
    """
    Turns checkbox option 'on' or 'true' to value_on (1)
    any other value returns value_off (0)
    """

    if isinstance(option, list):
        option = option[-1]

    if option in ('on', 'true'):
        return value_on

    return value_off


def clean_host(host, default_port=None):
    """
    Returns host or host:port or empty string from a given url or host
    If no port is found and default_port is given use host:default_port
    """

    host = host.strip()

    if host:

        match_host_port = re.search(r'(?:http.*://)?(?P<host>[^:/]+).?(?P<port>[0-9]*).*', host)

        cleaned_host = match_host_port.group('host')
        cleaned_port = match_host_port.group('port')

        if cleaned_host:

            if cleaned_port:
                host = cleaned_host + ':' + cleaned_port

            elif default_port:
                host = cleaned_host + ':' + str(default_port)

            else:
                host = cleaned_host

        else:
            host = ''

    return host


def clean_hosts(hosts, default_port=None):
    """
    Returns list of cleaned hosts by clean_host

    :param hosts: list of hosts
    :param default_port: default port to use
    :return: list of cleaned hosts
    """
    cleaned_hosts = []

    for cur_host in [host.strip() for host in hosts.split(',') if host.strip()]:
        cleaned_host = clean_host(cur_host, default_port)
        if cleaned_host:
            cleaned_hosts.append(cleaned_host)

    cleaned_hosts = cleaned_hosts or []

    return cleaned_hosts


def clean_url(url):
    """
    Returns an cleaned url starting with a scheme and folder with trailing /
    or an empty string
    """

    if url and url.strip():

        url = url.strip()

        if '://' not in url:
            url = '//' + url

        scheme, netloc, path, query, fragment = urlsplit(url, 'http')

        if not path:
            path += '/'

        cleaned_url = urlunsplit((scheme, netloc, path, query, fragment))

    else:
        cleaned_url = ''

    return cleaned_url


def convert_csv_string_to_list(value, delimiter=',', trim=False):
    """
    Convert comma or other character delimited strings to a list.

    :param value: The value to convert.f
    :param delimiter: Optionally Change the default delimiter ',' if required.
    :param trim: Optionally trim the individual list items.
    :return: The delimited value as a list.
    """

    if not isinstance(value, (string_types, text_type)):
        return value

    with suppress(AttributeError, ValueError):
        value = value.split(delimiter) if value else []
        if trim:
            value = [_.strip() for _ in value]

    return value


################################################################################
# Check_setting_int                                                            #
################################################################################
def minimax(val, default, low, high):
    """ Return value forced within range """

    val = try_int(val, default)

    if val < low:
        return low
    if val > high:
        return high

    return val


################################################################################
# Check_setting_int                                                            #
################################################################################
def check_setting_int(config, cfg_name, item_name, def_val, silent=True):
    try:
        my_val = config[cfg_name][item_name]
        if str(my_val).lower() == 'true':
            my_val = 1
        elif str(my_val).lower() == 'false':
            my_val = 0

        my_val = int(my_val)

        if str(my_val) == str(None):
            raise Exception
    except Exception:
        my_val = def_val
        try:
            config[cfg_name][item_name] = my_val
        except Exception:
            config[cfg_name] = {}
            config[cfg_name][item_name] = my_val

    if not silent:
        log.debug(u'{item} -> {value}', {u'item': item_name, u'value': my_val})

    return my_val


################################################################################
# Check_setting_bool                                                           #
################################################################################
def check_setting_bool(config, cfg_name, item_name, def_val, silent=True):
    return bool(check_setting_int(config=config, cfg_name=cfg_name, item_name=item_name, def_val=def_val, silent=silent))


################################################################################
# Check_setting_float                                                          #
################################################################################
def check_setting_float(config, cfg_name, item_name, def_val, silent=True):
    try:
        my_val = float(config[cfg_name][item_name])
        if str(my_val) == str(None):
            raise Exception
    except Exception:
        my_val = def_val
        try:
            config[cfg_name][item_name] = my_val
        except Exception:
            config[cfg_name] = {}
            config[cfg_name][item_name] = my_val

    if not silent:
        log.debug(u'{item} -> {value}', {u'item': item_name, u'value': my_val})

    return my_val


################################################################################
# Check_setting_str                                                            #
################################################################################
def check_setting_str(config, cfg_name, item_name, def_val, silent=True, censor_log=False, valid_values=None):
    # For passwords you must include the word `password` in the item_name
    # and add `helpers.encrypt(ITEM_NAME, ENCRYPTION_VERSION)` in save_config()
    if not censor_log:
        censor_level = common.privacy_levels['stupid']
    else:
        censor_level = common.privacy_levels[censor_log]
    privacy_level = common.privacy_levels[app.PRIVACY_LEVEL]
    if bool(item_name.find('password') + 1):
        encryption_version = app.ENCRYPTION_VERSION
    else:
        encryption_version = 0

    try:
        my_val = helpers.decrypt(config[cfg_name][item_name], encryption_version)
        if str(my_val) == str(None):
            raise Exception
    except Exception:
        my_val = def_val
        try:
            config[cfg_name][item_name] = helpers.encrypt(my_val, encryption_version)
        except Exception:
            config[cfg_name] = {}
            config[cfg_name][item_name] = helpers.encrypt(my_val, encryption_version)

    if privacy_level >= censor_level or (cfg_name, item_name) in iteritems(logger.censored_items):
        if not item_name.endswith('custom_url'):
            logger.censored_items[cfg_name, item_name] = my_val

    if not silent:
        log.debug(u'{item} -> {value}', {u'item': item_name, u'value': my_val})

    if valid_values and my_val not in valid_values:
        return def_val

    return my_val


################################################################################
# Check_setting_list                                                           #
################################################################################
def check_setting_list(config, cfg_name, item_name, default=None, silent=True, censor_log=False, transform=None, transform_default=0, split_value=False):
    """Check a setting, using the settings section and item name. Expect to return a list."""
    default = default or []

    if not censor_log:
        censor_level = common.privacy_levels['stupid']
    else:
        censor_level = common.privacy_levels[censor_log]
    privacy_level = common.privacy_levels[app.PRIVACY_LEVEL]

    try:
        my_val = config[cfg_name][item_name]
    except Exception:
        my_val = default
        try:
            config[cfg_name][item_name] = my_val
        except Exception:
            config[cfg_name] = {}
            config[cfg_name][item_name] = my_val

    if privacy_level >= censor_level or (cfg_name, item_name) in iteritems(logger.censored_items):
        if not item_name.endswith('custom_url'):
            logger.censored_items[cfg_name, item_name] = my_val

    if split_value:
        if isinstance(my_val, string_types):
            my_val = split_and_strip(my_val, split_value)

    # Make an attempt to cast the lists values.
    if isinstance(my_val, list) and transform:
        for index, value in enumerate(my_val):
            try:
                my_val[index] = transform(value)
            except ValueError:
                my_val[index] = transform_default

    if not silent:
        log.debug(u'{item} -> {value!r}', {u'item': item_name, u'value': my_val})

    return my_val


################################################################################
# Check_setting                                                                #
################################################################################
def check_setting(config, section, attr_type, attr, default=None, silent=True, **kwargs):
    """
    Check setting from config file
    """
    func = {
        'string': check_setting_str,
        'int': check_setting_int,
        'float': check_setting_float,
        'bool': check_setting_bool,
        'list': check_setting_list,
    }
    return func[attr_type](config, section, attr, default, silent, **kwargs)


################################################################################
# Check_setting                                                                #
################################################################################
def check_provider_setting(config, provider, attr_type, attr, default=None, silent=True, **kwargs):
    """
    Check setting from config file
    """
    name = provider.get_id()
    section = name.upper()
    attr = '{name}_{attr}'.format(name=name, attr=attr)
    return check_setting(config, section, attr_type, attr, default, silent, **kwargs)


################################################################################
# Load Provider Setting                                                        #
################################################################################
def load_provider_setting(config, provider, attr_type, attr, default=None, silent=True, **kwargs):
    if hasattr(provider, attr):
        value = check_provider_setting(config, provider, attr_type, attr, default, silent, **kwargs)
        setattr(provider, attr, value)


################################################################################
# Load Provider Setting                                                        #
################################################################################
def save_provider_setting(config, provider, attr, **kwargs):
    if hasattr(provider, attr):
        section = kwargs.pop('section', provider.get_id().upper())
        setting = '{name}_{attr}'.format(name=provider.get_id(), attr=attr)
        value = kwargs.pop('value', getattr(provider, attr))
        if value in [True, False]:
            value = int(value)
        config[section][setting] = value
