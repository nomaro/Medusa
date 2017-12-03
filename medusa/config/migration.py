# coding=utf-8

"""Migrates the config to the latest version."""

from __future__ import unicode_literals

import logging
import os
import re

from medusa import app, db, helpers, logger, naming
from medusa.config import (
    check_setting_int,
    check_setting_str,
    convert_csv_string_to_list,
    naming_sep_type
)
from medusa.logger.adapters.style import BraceAdapter


log = BraceAdapter(logging.getLogger(__name__))
log.logger.addHandler(logging.NullHandler())


class ConfigMigrator(object):
    def __init__(self, config_obj):
        """Migrates config to latest version."""

        self.config_obj = config_obj

        # check the version of the config
        self.config_version = check_setting_int(config_obj, 'General', 'config_version', app.CONFIG_VERSION)
        self.expected_config_version = app.CONFIG_VERSION
        self.migration_names = {
            1: 'Custom naming',
            2: 'Sync backup number with version number',
            3: 'Rename omgwtfnzb variables',
            4: 'Add newznab cat_ids',
            5: 'Metadata update',
            6: 'Convert from XBMC to new KODI variables',
            7: 'Use version 2 for password encryption',
            8: 'Convert Plex setting keys',
            9: 'Added setting "enable_manualsearch" for providers',
            10: 'Convert all csv config items to lists'
        }

    def migrate_config(self):
        """Migrate through each version until the config is updated."""

        if self.config_version > self.expected_config_version:
            logger.log_error_and_exit(
                """Your config version (%i) has been incremented past what this version of the application supports (%i).
                If you have used other forks or a newer version of the application, your config file may be unusable due to their modifications.""" %
                (self.config_version, self.expected_config_version)
            )

        app.CONFIG_VERSION = self.config_version

        while self.config_version < self.expected_config_version:
            next_version = self.config_version + 1

            if next_version in self.migration_names:
                migration_name = ': ' + self.migration_names[next_version]
            else:
                migration_name = ''

            log.info('Backing up config before upgrade')
            if not helpers.backup_versioned_file(app.CONFIG_FILE, self.config_version):
                logger.log_error_and_exit('Config backup failed, abort upgrading config')
            else:
                log.info('Proceeding with upgrade')

            # do the migration, expect a method named _migrate_v<num>
                log.info('Migrating config up to version {version} {migration_name}',
                         {'version': next_version, 'migration_name': migration_name})
            getattr(self, '_migrate_v' + str(next_version))()
            self.config_version = next_version

            # save new config after migration
            app.CONFIG_VERSION = self.config_version
            log.info('Saving config file to disk')
            app.instance.save_config()

    def _migrate_v1(self):
        """Create config template from old naming settings."""

        app.NAMING_PATTERN = self._name_to_pattern()
        log.info("Based on your old settings I'm setting your new naming pattern to: {pattern}",
                 {'pattern': app.NAMING_PATTERN})

        app.NAMING_CUSTOM_ABD = bool(check_setting_int(self.config_obj, 'General', 'naming_dates', 0))

        if app.NAMING_CUSTOM_ABD:
            app.NAMING_ABD_PATTERN = self._name_to_pattern(True)
            log.info('Adding a custom air-by-date naming pattern to your config: {pattern}',
                     {'pattern': app.NAMING_ABD_PATTERN})
        else:
            app.NAMING_ABD_PATTERN = naming.name_abd_presets[0]

        app.NAMING_MULTI_EP = int(check_setting_int(self.config_obj, 'General', 'naming_multi_ep_type', 1))

        # see if any of their shows used season folders
        main_db_con = db.DBConnection()
        season_folder_shows = main_db_con.select(b'SELECT indexer_id FROM tv_shows WHERE flatten_folders = 0 LIMIT 1')

        # if any shows had season folders on then prepend season folder to the pattern
        if season_folder_shows:

            old_season_format = check_setting_str(self.config_obj, 'General', 'season_folders_format', 'Season %02d')

            if old_season_format:
                try:
                    new_season_format = old_season_format % 9
                    new_season_format = str(new_season_format).replace('09', '%0S')
                    new_season_format = new_season_format.replace('9', '%S')

                    log.info(
                        'Changed season folder format from {old_season_format} to {new_season_format}, '
                        'prepending it to your naming config',
                        {'old_season_format': old_season_format, 'new_season_format': new_season_format}
                    )
                    app.NAMING_PATTERN = new_season_format + os.sep + app.NAMING_PATTERN

                except (TypeError, ValueError):
                    log.error("Can't change {old_season_format} to new season format",
                              {'old_season_format': old_season_format})

        # if no shows had it on then don't flatten any shows and don't put season folders in the config
        else:
            log.info("No shows were using season folders before so I'm disabling flattening on all shows")

            # don't flatten any shows at all
            main_db_con.action(b'UPDATE tv_shows SET flatten_folders = 0')

        app.NAMING_FORCE_FOLDERS = naming.check_force_season_folders()

    def _name_to_pattern(self, abd=False):

        # get the old settings from the file
        use_periods = bool(check_setting_int(self.config_obj, 'General', 'naming_use_periods', 0))
        ep_type = check_setting_int(self.config_obj, 'General', 'naming_ep_type', 0)
        sep_type = check_setting_int(self.config_obj, 'General', 'naming_sep_type', 0)
        use_quality = bool(check_setting_int(self.config_obj, 'General', 'naming_quality', 0))

        use_show_name = bool(check_setting_int(self.config_obj, 'General', 'naming_show_name', 1))
        use_ep_name = bool(check_setting_int(self.config_obj, 'General', 'naming_ep_name', 1))

        # make the presets into templates
        naming_ep_type = ('%Sx%0E',
                          's%0Se%0E',
                          'S%0SE%0E',
                          '%0Sx%0E')

        # set up our data to use
        if use_periods:
            show_name = '%S.N'
            ep_name = '%E.N'
            ep_quality = '%Q.N'
            abd_string = '%A.D'
        else:
            show_name = '%SN'
            ep_name = '%EN'
            ep_quality = '%QN'
            abd_string = '%A-D'

        if abd and abd_string:
            ep_string = abd_string
        else:
            ep_string = naming_ep_type[ep_type]

        finalName = ''

        # start with the show name
        if use_show_name and show_name:
            finalName += show_name + naming_sep_type[sep_type]

        # add the season/ep stuff
        finalName += ep_string

        # add the episode name
        if use_ep_name and ep_name:
            finalName += naming_sep_type[sep_type] + ep_name

        # add the quality
        if use_quality and ep_quality:
            finalName += naming_sep_type[sep_type] + ep_quality

        if use_periods:
            finalName = re.sub(r'\s+', '.', finalName)

        return finalName

    def _migrate_v2(self):
        """Dummy migration to sync backup number with config version number."""

    def _migrate_v3(self):
        """Rename omgwtfnzb variables."""
        # get the old settings from the file and store them in the new variable names
        app.OMGWTFNZBS_USERNAME = check_setting_str(self.config_obj, 'omgwtfnzbs', 'omgwtfnzbs_uid', '')
        app.OMGWTFNZBS_APIKEY = check_setting_str(self.config_obj, 'omgwtfnzbs', 'omgwtfnzbs_key', '')

    def _migrate_v4(self):
        """Add default newznab cat_ids and make them unique per provider."""

        new_newznab_data = []
        old_newznab_data = check_setting_str(self.config_obj, 'Newznab', 'newznab_data', '')

        if old_newznab_data:
            old_newznab_data_list = old_newznab_data.split('!!!')

            for cur_provider_data in old_newznab_data_list:
                try:
                    name, url, key, enabled = cur_provider_data.split('|')
                except ValueError:
                    log.error('Skipping Newznab provider string: {cur_provider_data!r}, incorrect format',
                              {'cur_provider_data': cur_provider_data})
                    continue

                if name == 'Sick Beard Index':
                    key = '0'

                if name == 'NZBs.org':
                    cat_ids = '5030,5040,5060,5070,5090'
                else:
                    cat_ids = '5030,5040,5060'

                cur_provider_data_list = [name, url, key, cat_ids, enabled]
                new_newznab_data.append('|'.join(cur_provider_data_list))

            app.NEWZNAB_DATA = '!!!'.join(new_newznab_data)

    def _migrate_v5(self):
        """Update metadata values to the new format.

        Quick overview of what the upgrade does:

        new | old | description (new)
        ----+-----+--------------------
          1 |  1  | show metadata
          2 |  2  | episode metadata
          3 |  4  | show fanart
          4 |  3  | show poster
          5 |  -  | show banner
          6 |  5  | episode thumb
          7 |  6  | season poster
          8 |  -  | season banner
          9 |  -  | season all poster
         10 |  -  | season all banner

        Note that the ini places start at 1 while the list index starts at 0.
        old format: 0|0|0|0|0|0 -- 6 places
        new format: 0|0|0|0|0|0|0|0|0|0 -- 10 places

        Drop the use of use_banner option.
        Migrate the poster override to just using the banner option for xbmc
        """

        metadata_xbmc = check_setting_str(self.config_obj, 'General', 'metadata_xbmc', '0|0|0|0|0|0')
        metadata_xbmc_12plus = check_setting_str(self.config_obj, 'General', 'metadata_xbmc_12plus', '0|0|0|0|0|0')
        metadata_mediabrowser = check_setting_str(self.config_obj, 'General', 'metadata_mediabrowser', '0|0|0|0|0|0')
        metadata_ps3 = check_setting_str(self.config_obj, 'General', 'metadata_ps3', '0|0|0|0|0|0')
        metadata_wdtv = check_setting_str(self.config_obj, 'General', 'metadata_wdtv', '0|0|0|0|0|0')
        metadata_tivo = check_setting_str(self.config_obj, 'General', 'metadata_tivo', '0|0|0|0|0|0')
        metadata_mede8er = check_setting_str(self.config_obj, 'General', 'metadata_mede8er', '0|0|0|0|0|0')

        use_banner = bool(check_setting_int(self.config_obj, 'General', 'use_banner', 0))

        def _migrate_metadata(metadata, metadata_name, use_banner):
            cur_metadata = metadata.split('|')
            # if target has the old number of values, do upgrade
            if len(cur_metadata) == 6:
                log.info('Upgrading {metadata_name} metadata, old value: {value}',
                         {'metadata_name': metadata_name, 'value': metadata})
                cur_metadata.insert(4, '0')
                cur_metadata.append('0')
                cur_metadata.append('0')
                cur_metadata.append('0')
                # swap show fanart, show poster
                cur_metadata[3], cur_metadata[2] = cur_metadata[2], cur_metadata[3]
                # if user was using use_banner to override the poster, instead enable the banner option and deactivate poster
                if metadata_name == 'XBMC' and use_banner:
                    cur_metadata[4], cur_metadata[3] = cur_metadata[3], '0'
                # write new format
                metadata = '|'.join(cur_metadata)
                log.info('Upgrading {metadata_name} metadata, new value: {value}',
                         {'metadata_name': metadata_name, 'value': metadata})

            elif len(cur_metadata) == 10:

                metadata = '|'.join(cur_metadata)
                log.info('Keeping {metadata_name} metadata, value: {value}',
                         {'metadata_name': metadata_name, 'value': metadata})

            else:
                log.error('Skipping {metadata_name} metadata {metadata!r}, incorrect format',
                          {'metadata_name': metadata_name, 'metadata': metadata})
                metadata = '0|0|0|0|0|0|0|0|0|0'
                log.info('Setting {metadata_name} metadata, new value: {value}',
                         {'metadata_name': metadata_name, 'value': metadata})

            return metadata

        app.METADATA_XBMC = _migrate_metadata(metadata_xbmc, 'XBMC', use_banner)
        app.METADATA_XBMC_12PLUS = _migrate_metadata(metadata_xbmc_12plus, 'XBMC 12+', use_banner)
        app.METADATA_MEDIABROWSER = _migrate_metadata(metadata_mediabrowser, 'MediaBrowser', use_banner)
        app.METADATA_PS3 = _migrate_metadata(metadata_ps3, 'PS3', use_banner)
        app.METADATA_WDTV = _migrate_metadata(metadata_wdtv, 'WDTV', use_banner)
        app.METADATA_TIVO = _migrate_metadata(metadata_tivo, 'TIVO', use_banner)
        app.METADATA_MEDE8ER = _migrate_metadata(metadata_mede8er, 'Mede8er', use_banner)

    def _migrate_v6(self):
        """Convert from XBMC to KODI variables."""
        app.USE_KODI = bool(check_setting_int(self.config_obj, 'XBMC', 'use_xbmc', 0))
        app.KODI_ALWAYS_ON = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_always_on', 1))
        app.KODI_NOTIFY_ONSNATCH = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_notify_onsnatch', 0))
        app.KODI_NOTIFY_ONDOWNLOAD = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_notify_ondownload', 0))
        app.KODI_NOTIFY_ONSUBTITLEDOWNLOAD = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_notify_onsubtitledownload', 0))
        app.KODI_UPDATE_LIBRARY = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_update_library', 0))
        app.KODI_UPDATE_FULL = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_update_full', 0))
        app.KODI_UPDATE_ONLYFIRST = bool(check_setting_int(self.config_obj, 'XBMC', 'xbmc_update_onlyfirst', 0))
        app.KODI_HOST = check_setting_str(self.config_obj, 'XBMC', 'xbmc_host', '')
        app.KODI_USERNAME = check_setting_str(self.config_obj, 'XBMC', 'xbmc_username', '', censor_log=True)
        app.KODI_PASSWORD = check_setting_str(self.config_obj, 'XBMC', 'xbmc_password', '', censor_log=True)
        app.METADATA_KODI = check_setting_str(self.config_obj, 'General', 'metadata_xbmc', '0|0|0|0|0|0|0|0|0|0')
        app.METADATA_KODI_12PLUS = check_setting_str(self.config_obj, 'General', 'metadata_xbmc_12plus', '0|0|0|0|0|0|0|0|0|0')

    def _migrate_v7(self):
        """Update password encryption to version 2."""
        app.ENCRYPTION_VERSION = 2

    def _migrate_v8(self):
        app.PLEX_CLIENT_HOST = check_setting_str(self.config_obj, 'Plex', 'plex_host', '')
        app.PLEX_SERVER_USERNAME = check_setting_str(self.config_obj, 'Plex', 'plex_username', '', censor_log=True)
        app.PLEX_SERVER_PASSWORD = check_setting_str(self.config_obj, 'Plex', 'plex_password', '', censor_log=True)
        app.USE_PLEX_SERVER = bool(check_setting_int(self.config_obj, 'Plex', 'use_plex', 0))

    def _migrate_v9(self):
        """Add 'enable_manualsearch' setting for providers"""

    def _migrate_v10(self):
        """
        Convert csv to lists in config.

        ConfigObj provides a way for storing lists. These are saved
        as comma separated values, using this the format documented here:
        http://configobj.readthedocs.io/en/latest/configobj.html?highlight=lists#list-values
        """

        def get_providers_from_data(providers_string):
            """Split provider string into providers and get the names."""
            return [provider.split('|')[0].upper() for provider in providers_string.split('!!!') if provider]

        def make_id(name):
            """Make ID of the provider."""
            if not name:
                return ''

            return re.sub(r'[^\w\d_]', '_', str(name).strip().upper())

        def get_rss_torrent_providers_list(data):
            """Get RSS torrent provider list."""
            providers_list = [_ for _ in (make_rss_torrent_provider(_) for _ in data.split('!!!')) if _]
            seen_values = set()
            providers_set = []

            for provider in providers_list:
                value = provider.name

                if value not in seen_values:
                    providers_set.append(provider)
                    seen_values.add(value)

            return [_ for _ in providers_set if _]

        def make_rss_torrent_provider(config):
            """Create new RSS provider."""
            if not config:
                return None

            cookies = ''
            enable_backlog = 0
            enable_daily = 0
            enable_manualsearch = 0
            search_fallback = 0
            search_mode = 'eponly'
            title_tag = 'title'

            try:
                values = config.split('|')

                if len(values) == 9:
                    name, url, cookies, title_tag, enabled, search_mode, search_fallback, enable_daily, enable_backlog = values
                elif len(values) == 10:
                    name, url, cookies, title_tag, enabled, search_mode, search_fallback, enable_daily, enable_backlog, enable_manualsearch = values
                elif len(values) == 8:
                    name, url, cookies, enabled, search_mode, search_fallback, enable_daily, enable_backlog = values
                else:
                    enabled = values[4]
                    name = values[0]
                    url = values[1]
            except ValueError:
                log.error('Skipping RSS Torrent provider string: {config}, incorrect format', {'config': config})
                return None

            new_provider = TorrentRssProvider(
                name, url, cookies=cookies, title_tag=title_tag, search_mode=search_mode,
                search_fallback=search_fallback,
                enable_daily=enable_daily, enable_backlog=enable_backlog, enable_manualsearch=enable_manualsearch
            )
            new_provider.enabled = enabled == '1'

            return new_provider

        # General
        app.GIT_RESET_BRANCHES = convert_csv_string_to_list(self.config_obj['General']['git_reset_branches'])
        app.ALLOWED_EXTENSIONS = convert_csv_string_to_list(self.config_obj['General']['allowed_extensions'])
        app.PROVIDER_ORDER = convert_csv_string_to_list(self.config_obj['General']['provider_order'], ' ')
        app.ROOT_DIRS = convert_csv_string_to_list(self.config_obj['General']['root_dirs'], '|')
        app.SYNC_FILES = convert_csv_string_to_list(self.config_obj['General']['sync_files'])
        app.IGNORE_WORDS = convert_csv_string_to_list(self.config_obj['General']['ignore_words'])
        app.PREFERRED_WORDS = convert_csv_string_to_list(self.config_obj['General']['preferred_words'])
        app.UNDESIRED_WORDS = convert_csv_string_to_list(self.config_obj['General']['undesired_words'])
        app.TRACKERS_LIST = convert_csv_string_to_list(self.config_obj['General']['trackers_list'])
        app.REQUIRE_WORDS = convert_csv_string_to_list(self.config_obj['General']['require_words'])
        app.IGNORED_SUBS_LIST = convert_csv_string_to_list(self.config_obj['General']['ignored_subs_list'])
        app.BROKEN_PROVIDERS = convert_csv_string_to_list(self.config_obj['General']['broken_providers'])
        app.EXTRA_SCRIPTS = convert_csv_string_to_list(self.config_obj['General']['extra_scripts'])

        # Metadata
        app.METADATA_KODI = convert_csv_string_to_list(self.config_obj['General']['metadata_kodi'], '|')
        app.METADATA_KODI_12PLUS = convert_csv_string_to_list(self.config_obj['General']['metadata_kodi_12plus'], '|')
        app.METADATA_MEDIABROWSER = convert_csv_string_to_list(self.config_obj['General']['metadata_mediabrowser'], '|')
        app.METADATA_PS3 = convert_csv_string_to_list(self.config_obj['General']['metadata_ps3'], '|')
        app.METADATA_WDTV = convert_csv_string_to_list(self.config_obj['General']['metadata_wdtv'], '|')
        app.METADATA_TIVO = convert_csv_string_to_list(self.config_obj['General']['metadata_tivo'], '|')
        app.METADATA_MEDE8ER = convert_csv_string_to_list(self.config_obj['General']['metadata_mede8er'], '|')

        # Subtitles
        app.SUBTITLES_LANGUAGES = convert_csv_string_to_list(self.config_obj['Subtitles']['subtitles_languages'])
        app.SUBTITLES_SERVICES_LIST = convert_csv_string_to_list(self.config_obj['Subtitles']['SUBTITLES_SERVICES_LIST'])
        app.SUBTITLES_SERVICES_ENABLED = convert_csv_string_to_list(self.config_obj['Subtitles']['SUBTITLES_SERVICES_ENABLED'], '|')

        # Notifications
        app.KODI_HOST = convert_csv_string_to_list(self.config_obj['KODI']['kodi_host'])
        app.PLEX_SERVER_HOST = convert_csv_string_to_list(self.config_obj['Plex']['plex_server_host'])
        app.PLEX_CLIENT_HOST = convert_csv_string_to_list(self.config_obj['Plex']['plex_client_host'])
        app.PROWL_API = convert_csv_string_to_list(self.config_obj['Prowl']['prowl_api'])
        app.PUSHOVER_DEVICE = convert_csv_string_to_list(self.config_obj['Pushover']['pushover_device'])
        app.NMA_API = convert_csv_string_to_list(self.config_obj['NMA']['nma_api'])
        app.EMAIL_LIST = convert_csv_string_to_list(self.config_obj['Email']['email_list'])

        try:
            # migrate rsstorrent providers
            from medusa.providers.torrent.rss.rsstorrent import TorrentRssProvider

            # Create the new list of torrent rss providers, with only the id stored.
            app.TORRENTRSS_PROVIDERS = get_providers_from_data(self.config_obj['TorrentRss']['torrentrss_data'])

            # Create the torrent providers from the old rsstorrent piped separated data.
            app.torrentRssProviderList = get_rss_torrent_providers_list(self.config_obj['TorrentRss']['torrentrss_data'])
        except KeyError:
            app.TORRENTRSS_PROVIDERS = []

        try:
            # migrate newznab providers.
            # Newznabprovider needs to be imported lazy, as the module will also import other providers to early.
            from medusa.providers.nzb.newznab import NewznabProvider

            # Create the newznab providers from the old newznab piped separated data.
            app.newznabProviderList = NewznabProvider.get_providers_list(
                self.config_obj['Newznab']['newznab_data']
            )

            app.NEWZNAB_PROVIDERS = [make_id(provider.name) for provider in app.newznabProviderList if not provider.default]
        except KeyError:
            app.NEWZNAB_PROVIDERS = []
