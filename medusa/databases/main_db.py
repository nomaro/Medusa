# coding=utf-8

from __future__ import unicode_literals

import datetime
import logging
import os.path
import sys
import warnings

from medusa import common, db, helpers, subtitles
from medusa.helper.common import dateTimeFormat, episode_num
from medusa.indexers.indexer_config import STATUS_MAP
from medusa.logger.adapters.style import BraceAdapter
from medusa.name_parser.parser import NameParser

from six import iteritems

log = BraceAdapter(logging.getLogger(__name__))
log.logger.addHandler(logging.NullHandler())

MIN_DB_VERSION = 40  # oldest db version we support migrating from
MAX_DB_VERSION = 44

# Used to check when checking for updates
CURRENT_MINOR_DB_VERSION = 8


class MainSanityCheck(db.DBSanityCheck):
    def check(self):
        self.fix_missing_table_indexes()
        self.fix_duplicate_shows()
        self.fix_duplicate_episodes()
        self.fix_orphan_episodes()
        self.fix_unaired_episodes()
        self.fix_indexer_show_statues()
        self.fix_episode_statuses()
        self.fix_invalid_airdates()
        #  self.fix_subtitles_codes()
        self.fix_show_nfo_lang()
        self.convert_archived_to_compound()
        self.fix_subtitle_reference()
        self.clean_null_indexer_mappings()

    def clean_null_indexer_mappings(self):
        log.debug('Checking for null indexer mappings')
        query = "SELECT * from indexer_mapping where mindexer_id = ''"

        sql_results = self.connection.select(query)
        if sql_results:
            log.debug('Found {0} null indexer mapping. Deleting...',
                      len(sql_results))
            self.connection.action("DELETE FROM indexer_mapping WHERE mindexer_id = ''")

    def update_old_propers(self):
        # This is called once when we create proper_tags columns
        log.debug('Checking for old propers without proper tags')
        query = "SELECT resource FROM history WHERE (proper_tags is null or proper_tags is '') " + \
                "AND (action LIKE '%2' OR action LIKE '%9') AND " + \
                "(resource LIKE '%REPACK%' or resource LIKE '%PROPER%' or resource LIKE '%REAL%')"
        sql_results = self.connection.select(query)
        if sql_results:
            for sql_result in sql_results:
                proper_release = sql_result[b'resource']
                log.debug('Found old propers without proper tags: {0}',
                          proper_release)
                parse_result = NameParser()._parse_string(proper_release)
                if parse_result.proper_tags:
                    proper_tags = '|'.join(parse_result.proper_tags)
                    log.debug('Add proper tags {0!r} to {1!r}',
                              proper_tags, proper_release)
                    self.connection.action('UPDATE history SET proper_tags = ? WHERE resource = ?',
                                           [proper_tags, proper_release])

    def fix_subtitle_reference(self):
        log.debug('Checking for delete episodes with subtitle reference')
        query = "SELECT episode_id, showid, location, subtitles, subtitles_searchcount, subtitles_lastsearch " + \
                "FROM tv_episodes WHERE location = '' AND subtitles is not ''"

        sql_results = self.connection.select(query)
        if sql_results:
            for sql_result in sql_results:
                log.warning('Found deleted episode id {0} from show ID {1}'
                            ' with subtitle data. Erasing reference...',
                            sql_result[b'episode_id'], sql_result[b'showid'])
                self.connection.action("UPDATE tv_episodes SET subtitles = '', subtitles_searchcount = 0, subtitles_lastsearch = '' " +
                                       "WHERE episode_id = %i" % (sql_result[b'episode_id']))

    def convert_archived_to_compound(self):
        log.debug('Checking for archived episodes not qualified')

        query = 'SELECT episode_id, showid, e.status, e.location, season, episode, anime ' + \
                'FROM tv_episodes e, tv_shows s WHERE e.status = %s AND e.showid = s.indexer_id' % common.ARCHIVED

        sql_results = self.connection.select(query)
        if sql_results:
            log.warning('Found {0} shows with bare archived status, '
                        'attempting automatic conversion...',
                        len(sql_results))

        for archived_ep in sql_results:
            fixed_status = common.Quality.composite_status(common.ARCHIVED, common.Quality.UNKNOWN)
            existing = archived_ep[b'location'] and os.path.exists(archived_ep[b'location'])
            if existing:
                quality = common.Quality.name_quality(archived_ep[b'location'], archived_ep[b'anime'], extend=False)
                fixed_status = common.Quality.composite_status(common.ARCHIVED, quality)

            log.info(
                'Changing status from {old_status} to {new_status} for'
                ' {id}: {ep} at {location} (File {result})',
                {'old_status': common.statusStrings[common.ARCHIVED],
                 'new_status': common.statusStrings[fixed_status],
                 'id': archived_ep[b'showid'],
                 'ep': episode_num(archived_ep[b'season'],
                                   archived_ep[b'episode']),
                 'location': archived_ep[b'location'] or 'unknown location',
                 'result': 'EXISTS' if existing else 'NOT FOUND', }
            )

            self.connection.action('UPDATE tv_episodes SET status = %i WHERE episode_id = %i' % (fixed_status, archived_ep[b'episode_id']))

    def fix_duplicate_shows(self, column='indexer_id'):
        sql_results = self.connection.select(
            'SELECT show_id, ' + column + ', COUNT(' + column + ') as count FROM tv_shows GROUP BY ' + column + ' HAVING count > 1')

        for cur_duplicate in sql_results:

            log.info('Duplicate show detected! {0}: {1!s} count: {2!s}',
                     column, cur_duplicate[column], cur_duplicate[b'count'])

            cur_dupe_results = self.connection.select(
                'SELECT show_id, ' + column + ' FROM tv_shows WHERE ' + column + ' = ? LIMIT ?',
                [cur_duplicate[column], int(cur_duplicate[b'count']) - 1]
            )

            for cur_dupe_id in cur_dupe_results:
                log.info('Deleting duplicate show with {0}: {1!s}'
                         ' show_id: {2!s}', column, cur_dupe_id[column],
                         cur_dupe_id[b'show_id'])
                self.connection.action('DELETE FROM tv_shows WHERE show_id = ?', [cur_dupe_id[b'show_id']])

    def fix_duplicate_episodes(self):

        sql_results = self.connection.select(
            'SELECT showid, season, episode, COUNT(showid) as count FROM tv_episodes GROUP BY showid, season, episode HAVING count > 1')

        for cur_duplicate in sql_results:

            log.debug('Duplicate episode detected! showid: {0!s}'
                      ' season: {1!s} episode: {2!s} count: {3!s}',
                      cur_duplicate[b'showid'], cur_duplicate[b'season'],
                      cur_duplicate[b'episode'], cur_duplicate[b'count'])
            cur_dupe_results = self.connection.select(
                'SELECT episode_id FROM tv_episodes WHERE showid = ? AND season = ? and episode = ? ORDER BY episode_id DESC LIMIT ?',
                [cur_duplicate[b'showid'], cur_duplicate[b'season'], cur_duplicate[b'episode'],
                 int(cur_duplicate[b'count']) - 1]
            )

            for cur_dupe_id in cur_dupe_results:
                log.info('Deleting duplicate episode with episode_id: {0!s}',
                         cur_dupe_id[b'episode_id'])
                self.connection.action('DELETE FROM tv_episodes WHERE episode_id = ?', [cur_dupe_id[b'episode_id']])

    def fix_orphan_episodes(self):

        sql_results = self.connection.select(
            'SELECT episode_id, showid, tv_shows.indexer_id FROM tv_episodes LEFT JOIN tv_shows ON tv_episodes.showid=tv_shows.indexer_id WHERE tv_shows.indexer_id is NULL')

        for cur_orphan in sql_results:
            log.debug('Orphan episode detected! episode_id: {0!s}'
                      ' showid: {1!s}', cur_orphan[b'episode_id'],
                      cur_orphan[b'showid'])
            log.info('Deleting orphan episode with episode_id: {0!s}',
                     cur_orphan[b'episode_id'])
            self.connection.action('DELETE FROM tv_episodes WHERE episode_id = ?', [cur_orphan[b'episode_id']])

    def fix_missing_table_indexes(self):
        if not self.connection.select("PRAGMA index_info('idx_indexer_id')"):
            log.info('Missing idx_indexer_id for TV Shows table detected!,'
                     ' fixing...')
            self.connection.action('CREATE UNIQUE INDEX idx_indexer_id ON tv_shows(indexer_id);')

        if not self.connection.select("PRAGMA index_info('idx_tv_episodes_showid_airdate')"):
            log.info('Missing idx_tv_episodes_showid_airdate for TV Episodes'
                     ' table detected!, fixing...')
            self.connection.action('CREATE INDEX idx_tv_episodes_showid_airdate ON tv_episodes(showid, airdate);')

        if not self.connection.select("PRAGMA index_info('idx_showid')"):
            log.info('Missing idx_showid for TV Episodes table detected!,'
                     ' fixing...')
            self.connection.action('CREATE INDEX idx_showid ON tv_episodes (showid);')

        if not self.connection.select("PRAGMA index_info('idx_status')"):
            log.info('Missing idx_status for TV Episodes table detected!,'
                     ' fixing...')
            self.connection.action('CREATE INDEX idx_status ON tv_episodes (status, season, episode, airdate)')

        if not self.connection.select("PRAGMA index_info('idx_sta_epi_air')"):
            log.info('Missing idx_sta_epi_air for TV Episodes table'
                     ' detected!, fixing...')
            self.connection.action('CREATE INDEX idx_sta_epi_air ON tv_episodes (status, episode, airdate)')

        if not self.connection.select("PRAGMA index_info('idx_sta_epi_sta_air')"):
            log.info('Missing idx_sta_epi_sta_air for TV Episodes table'
                     ' detected!, fixing...')
            self.connection.action('CREATE INDEX idx_sta_epi_sta_air ON tv_episodes (season, episode, status, airdate)')

    def fix_unaired_episodes(self):

        cur_date = datetime.date.today()

        sql_results = self.connection.select(
            'SELECT episode_id FROM tv_episodes WHERE (airdate > ? or airdate = 1) AND status in (?,?) AND season > 0',
            [cur_date.toordinal(), common.SKIPPED, common.WANTED])

        for cur_unaired in sql_results:
            log.info('Fixing unaired episode status for episode_id: {0!s}',
                     cur_unaired[b'episode_id'])
            self.connection.action('UPDATE tv_episodes SET status = ? WHERE episode_id = ?',
                                   [common.UNAIRED, cur_unaired[b'episode_id']])

    def fix_indexer_show_statues(self):
        for old_status, new_status in iteritems(STATUS_MAP):
            self.connection.action('UPDATE tv_shows SET status = ? WHERE LOWER(status) = ?', [new_status, old_status])

    def fix_episode_statuses(self):
        sql_results = self.connection.select('SELECT episode_id, showid FROM tv_episodes WHERE status IS NULL')

        for cur_ep in sql_results:
            log.debug('MALFORMED episode status detected! episode_id: {0!s}'
                      ' showid: {1!s}', cur_ep[b'episode_id'],
                      cur_ep[b'showid'])
            log.info('Fixing malformed episode status with'
                     ' episode_id: {0!s}', cur_ep[b'episode_id'])
            self.connection.action('UPDATE tv_episodes SET status = ? WHERE episode_id = ?',
                                   [common.UNKNOWN, cur_ep[b'episode_id']])

    def fix_invalid_airdates(self):

        sql_results = self.connection.select(
            'SELECT episode_id, showid FROM tv_episodes WHERE airdate >= ? OR airdate < 1',
            [datetime.date.max.toordinal()])

        for bad_airdate in sql_results:
            log.debug('Bad episode airdate detected! episode_id: {0!s}'
                      ' showid: {1!s}', bad_airdate[b'episode_id'],
                      bad_airdate[b'showid'])
            log.info('Fixing bad episode airdate for episode_id: {0!s}',
                     bad_airdate[b'episode_id'])
            self.connection.action("UPDATE tv_episodes SET airdate = '1' WHERE episode_id = ?", [bad_airdate[b'episode_id']])

    def fix_subtitles_codes(self):

        sql_results = self.connection.select(
            "SELECT subtitles, episode_id FROM tv_episodes WHERE subtitles != '' AND subtitles_lastsearch < ?;",
            [datetime.datetime(2015, 7, 15, 17, 20, 44, 326380).strftime(dateTimeFormat)]
        )

        if not sql_results:
            return

        for sql_result in sql_results:
            langs = []

            log.debug('Checking subtitle codes for episode_id: {0!s},'
                      ' codes: {1!s}', sql_result[b'episode_id'],
                      sql_result[b'subtitles'])

            for subcode in sql_result[b'subtitles'].split(','):
                if not len(subcode) == 3 or subcode not in subtitles.subtitle_code_filter():
                    log.debug('Fixing subtitle codes for episode_id: {0!s},'
                              ' invalid code: {1!s}',
                              sql_result[b'episode_id'], subcode)
                    continue

                langs.append(subcode)

            self.connection.action('UPDATE tv_episodes SET subtitles = ?, subtitles_lastsearch = ? WHERE episode_id = ?;',
                                   [','.join(langs), datetime.datetime.now().strftime(dateTimeFormat), sql_result[b'episode_id']])

    def fix_show_nfo_lang(self):
        self.connection.action("UPDATE tv_shows SET lang = '' WHERE lang = 0 or lang = '0'")


def backupDatabase(version):
    log.info('Backing up database before upgrade')
    if not helpers.backup_versioned_file(db.dbFilename(), version):
        log.error('Database backup failed, abort upgrading database')
        sys.exit(1)
    else:
        log.info('Proceeding with upgrade')


# ======================
# = Main DB Migrations =
# ======================
# Add new migrations at the bottom of the list; subclass the previous migration.

class InitialSchema(db.SchemaUpgrade):
    def test(self):
        return self.hasTable('db_version')

    def execute(self):
        if not self.hasTable('tv_shows') and not self.hasTable('db_version'):
            queries = [
                'CREATE TABLE db_version(db_version INTEGER);',
                'CREATE TABLE history(action NUMERIC, date NUMERIC, showid NUMERIC, season NUMERIC, episode NUMERIC, quality NUMERIC, resource TEXT, provider TEXT, version NUMERIC DEFAULT -1);',
                'CREATE TABLE imdb_info(indexer_id INTEGER PRIMARY KEY, imdb_id TEXT, title TEXT, year NUMERIC, akas TEXT, runtimes NUMERIC, genres TEXT, countries TEXT, country_codes TEXT, certificates TEXT, rating TEXT, votes INTEGER, last_update NUMERIC, plot TEXT);',
                'CREATE TABLE info(last_backlog NUMERIC, last_indexer NUMERIC, last_proper_search NUMERIC);',
                'CREATE TABLE scene_numbering(indexer TEXT, indexer_id INTEGER, season INTEGER, episode INTEGER, scene_season INTEGER, scene_episode INTEGER, absolute_number NUMERIC, scene_absolute_number NUMERIC, PRIMARY KEY(indexer_id, season, episode));',
                'CREATE TABLE tv_shows(show_id INTEGER PRIMARY KEY, indexer_id NUMERIC, indexer NUMERIC, show_name TEXT, location TEXT, network TEXT, genre TEXT, classification TEXT, runtime NUMERIC, quality NUMERIC, airs TEXT, status TEXT, flatten_folders NUMERIC, paused NUMERIC, startyear NUMERIC, air_by_date NUMERIC, lang TEXT, subtitles NUMERIC, notify_list TEXT, imdb_id TEXT, last_update_indexer NUMERIC, dvdorder NUMERIC, archive_firstmatch NUMERIC, rls_require_words TEXT, rls_ignore_words TEXT, sports NUMERIC, anime NUMERIC, scene NUMERIC, default_ep_status NUMERIC DEFAULT -1);',
                'CREATE TABLE tv_episodes(episode_id INTEGER PRIMARY KEY, showid NUMERIC, indexerid INTEGER, indexer INTEGER, name TEXT, season NUMERIC, episode NUMERIC, description TEXT, airdate NUMERIC, hasnfo NUMERIC, hastbn NUMERIC, status NUMERIC, location TEXT, file_size NUMERIC, release_name TEXT, subtitles TEXT, subtitles_searchcount NUMERIC, subtitles_lastsearch TIMESTAMP, is_proper NUMERIC, scene_season NUMERIC, scene_episode NUMERIC, absolute_number NUMERIC, scene_absolute_number NUMERIC, version NUMERIC DEFAULT -1, release_group TEXT);',
                'CREATE TABLE blacklist (show_id INTEGER, range TEXT, keyword TEXT);',
                'CREATE TABLE whitelist (show_id INTEGER, range TEXT, keyword TEXT);',
                'CREATE TABLE xem_refresh (indexer TEXT, indexer_id INTEGER PRIMARY KEY, last_refreshed INTEGER);',
                'CREATE TABLE indexer_mapping (indexer_id INTEGER, indexer INTEGER, mindexer_id INTEGER, mindexer INTEGER, PRIMARY KEY (indexer_id, indexer, mindexer));',
                'CREATE UNIQUE INDEX idx_indexer_id ON tv_shows(indexer_id);',
                'CREATE INDEX idx_showid ON tv_episodes(showid);',
                'CREATE INDEX idx_sta_epi_air ON tv_episodes(status, episode, airdate);',
                'CREATE INDEX idx_sta_epi_sta_air ON tv_episodes(season, episode, status, airdate);',
                'CREATE INDEX idx_status ON tv_episodes(status, season, episode, airdate);',
                'CREATE INDEX idx_tv_episodes_showid_airdate ON tv_episodes(showid, airdate);',
                'INSERT INTO db_version(db_version) VALUES (42);'
            ]
            for query in queries:
                self.connection.action(query)

        else:
            cur_db_version = self.checkDBVersion()

            if cur_db_version < MIN_DB_VERSION:
                log.error(
                    'Your database version ({0!s}) is too old to migrate'
                    ' from what this version of the application'
                    ' supports ({1!s}).\n'
                    'Upgrade using a previous version (tag) build 496 to'
                    ' build 501 of the application first or remove database'
                    ' file to begin fresh.', cur_db_version, MIN_DB_VERSION,
                )
                sys.exit(1)

            if cur_db_version > MAX_DB_VERSION:
                log.error(
                    'Your database version ({0!s}) has been incremented past'
                    ' what this version of the application supports'
                    ' ({1!s}).\n'
                    'If you have used other forks of the application, your'
                    ' database may be unusable due to their modifications.',
                    cur_db_version, MAX_DB_VERSION,
                )


class AddVersionToTvEpisodes(InitialSchema):
    def test(self):
        return self.checkDBVersion() >= 40

    def execute(self):
        backupDatabase(self.checkDBVersion())

        log.info('Adding column version to tv_episodes and history')
        self.addColumn('tv_episodes', 'version', 'NUMERIC', '-1')
        self.addColumn('tv_episodes', 'release_group', 'TEXT', '')
        self.addColumn('history', 'version', 'NUMERIC', '-1')

        self.incDBVersion()


class AddDefaultEpStatusToTvShows(AddVersionToTvEpisodes):
    def test(self):
        return self.checkDBVersion() >= 41

    def execute(self):
        backupDatabase(self.checkDBVersion())

        log.info('Adding column default_ep_status to tv_shows')
        self.addColumn('tv_shows', 'default_ep_status', 'NUMERIC', '-1')

        self.incDBVersion()


class AlterTVShowsFieldTypes(AddDefaultEpStatusToTvShows):
    def test(self):
        return self.checkDBVersion() >= 42

    def execute(self):
        backupDatabase(self.checkDBVersion())

        log.info('Converting column indexer and default_ep_status field types to numeric')
        self.connection.action('DROP TABLE IF EXISTS tmp_tv_shows')
        self.connection.action('ALTER TABLE tv_shows RENAME TO tmp_tv_shows')
        self.connection.action('CREATE TABLE tv_shows (show_id INTEGER PRIMARY KEY, indexer_id NUMERIC, indexer NUMERIC, show_name TEXT, location TEXT, network TEXT, genre TEXT, classification TEXT, runtime NUMERIC, quality NUMERIC, airs TEXT, status TEXT, flatten_folders NUMERIC, paused NUMERIC, startyear NUMERIC, air_by_date NUMERIC, lang TEXT, subtitles NUMERIC, notify_list TEXT, imdb_id TEXT, last_update_indexer NUMERIC, dvdorder NUMERIC, archive_firstmatch NUMERIC, rls_require_words TEXT, rls_ignore_words TEXT, sports NUMERIC, anime NUMERIC, scene NUMERIC, default_ep_status NUMERIC)')
        self.connection.action('INSERT INTO tv_shows SELECT * FROM tmp_tv_shows')
        self.connection.action('DROP TABLE tmp_tv_shows')

        self.incDBVersion()


class AddMinorVersion(AlterTVShowsFieldTypes):
    def test(self):
        return self.checkDBVersion() >= 42 and self.hasColumn(b'db_version', b'db_minor_version')

    def incDBVersion(self):
        warnings.warn('Deprecated: Use inc_major_version or inc_minor_version instead', DeprecationWarning)

    def inc_major_version(self):
        major_version, minor_version = self.connection.version
        major_version += 1
        minor_version = 0
        self.connection.action('UPDATE db_version SET db_version = ?, db_minor_version = ?', [major_version, minor_version])
        return self.connection.version

    def inc_minor_version(self):
        major_version, minor_version = self.connection.version
        minor_version += 1
        self.connection.action('UPDATE db_version SET db_version = ?, db_minor_version = ?', [major_version, minor_version])
        return self.connection.version

    def execute(self):
        backupDatabase(self.checkDBVersion())

        log.info('Add minor version numbers to database')
        self.addColumn(b'db_version', b'db_minor_version')

        self.inc_minor_version()

        log.info('Updated to: {}.{}', *self.connection.version)


class TestIncreaseMajorVersion(AddMinorVersion):
    """
    This tests the inc_major_version function.

    This is done both to test the new update functionality
    and to maintain version parity with other forks.
    """

    def test(self):
        """Test if the version is < 44.0."""
        return self.connection.version >= (44, 0)

    def execute(self):
        """Update the version until 44.1."""
        backupDatabase(self.connection.version)

        log.info('Test major and minor version updates database')
        self.inc_major_version()
        self.inc_minor_version()

        log.info('Updated to: {}.{}', *self.connection.version)


class AddProperTags(TestIncreaseMajorVersion):
    """Adds column proper_tags to history table."""

    def test(self):
        """Test if the version is < 44.2."""
        return self.connection.version >= (44, 2)

    def execute(self):
        """Update the version until 44.2 and add proper_tags column."""
        backupDatabase(self.connection.version)

        if not self.hasColumn('history', 'proper_tags'):
            log.info('Adding column proper_tags to history')
            self.addColumn('history', 'proper_tags', 'TEXT', '')

        # Call the update old propers once
        MainSanityCheck(self.connection).update_old_propers()
        self.inc_minor_version()

        log.info('Updated to: {}.{}', *self.connection.version)


class AddManualSearched(AddProperTags):
    """Adds columns manually_searched to history and tv_episodes table."""

    def test(self):
        """Test if the version is < 44.3."""
        return self.connection.version >= (44, 3)

    def execute(self):
        """Update the version until 44.3 and add manually_searched columns."""
        backupDatabase(self.connection.version)

        if not self.hasColumn('history', 'manually_searched'):
            log.info('Adding column manually_searched to history')
            self.addColumn('history', 'manually_searched', 'NUMERIC', 0)

        if not self.hasColumn('tv_episodes', 'manually_searched'):
            log.info('Adding column manually_searched to tv_episodes')
            self.addColumn('tv_episodes', 'manually_searched', 'NUMERIC', 0)

        MainSanityCheck(self.connection).update_old_propers()
        self.inc_minor_version()

        log.info('Updated to: {}.{}', *self.connection.version)


class AddInfoHash(AddManualSearched):
    """Adds column info_hash to history table."""

    def test(self):
        """Test if the version is at least 44.4."""
        return self.connection.version >= (44, 4)

    def execute(self):
        backupDatabase(self.connection.version)

        log.info('Adding column info_hash in history')
        if not self.hasColumn('history', 'info_hash'):
            self.addColumn('history', 'info_hash', 'TEXT', None)
        self.inc_minor_version()


class AddPlot(AddInfoHash):
    """Adds column plot to imdb_info table."""

    def test(self):
        """Test if the version is at least 44.5."""
        return self.connection.version >= (44, 5)

    def execute(self):
        backupDatabase(self.connection.version)

        log.info('Adding column plot in imdb_info')
        if not self.hasColumn('imdb_info', 'plot'):
            self.addColumn('imdb_info', 'plot', 'TEXT', None)

        log.info('Adding column plot in tv_show')
        if not self.hasColumn('tv_shows', 'plot'):
            self.addColumn('tv_shows', 'plot', 'TEXT', None)
        self.inc_minor_version()


class AddResourceSize(AddPlot):
    """Adds column size to history table."""

    def test(self):
        """Test if the version is at least 44.6."""
        return self.connection.version >= (44, 6)

    def execute(self):
        backupDatabase(self.connection.version)

        log.info('Adding column size in history')
        if not self.hasColumn('history', 'size'):
            self.addColumn('history', 'size', 'NUMERIC', -1)

        self.inc_minor_version()


class AddPKIndexerMapping(AddResourceSize):
    """Add PK to mindexer column in indexer_mapping table."""

    def test(self):
        """Test if the version is at least 44.7."""
        return self.connection.version >= (44, 7)

    def execute(self):
        backupDatabase(self.connection.version)

        log.info('Adding PK to mindexer column in indexer_mapping table')
        self.connection.action('DROP TABLE IF EXISTS new_indexer_mapping;')
        self.connection.action('CREATE TABLE IF NOT EXISTS new_indexer_mapping'
                               '(indexer_id INTEGER, indexer INTEGER, mindexer_id INTEGER, mindexer INTEGER,'
                               'PRIMARY KEY (indexer_id, indexer, mindexer));')
        self.connection.action('INSERT INTO new_indexer_mapping SELECT * FROM indexer_mapping;')
        self.connection.action('DROP TABLE IF EXISTS indexer_mapping;')
        self.connection.action('ALTER TABLE new_indexer_mapping RENAME TO indexer_mapping;')
        self.connection.action('DROP TABLE IF EXISTS new_indexer_mapping;')
        self.inc_minor_version()


class AddIndexerInteger(AddPKIndexerMapping):
    """Make indexer as INTEGER in tv_episodes table."""

    def test(self):
        """Test if the version is at least 44.8."""
        return self.connection.version >= (44, 8)

    def execute(self):
        backupDatabase(self.connection.version)

        log.info('Make indexer and indexer_id as INTEGER in tv_episodes table')
        self.connection.action('DROP TABLE IF EXISTS new_tv_episodes;')
        self.connection.action('CREATE TABLE new_tv_episodes(episode_id INTEGER PRIMARY KEY, showid NUMERIC,'
                               'indexerid INTEGER, indexer INTEGER, name TEXT, season NUMERIC, episode NUMERIC,'
                               'description TEXT, airdate NUMERIC, hasnfo NUMERIC, hastbn NUMERIC, status NUMERIC,'
                               'location TEXT, file_size NUMERIC, release_name TEXT, subtitles TEXT,'
                               'subtitles_searchcount NUMERIC, subtitles_lastsearch TIMESTAMP,'
                               'is_proper NUMERIC, scene_season NUMERIC, scene_episode NUMERIC,'
                               'absolute_number NUMERIC, scene_absolute_number NUMERIC, version NUMERIC DEFAULT -1,'
                               'release_group TEXT, manually_searched NUMERIC);')
        self.connection.action('INSERT INTO new_tv_episodes SELECT * FROM tv_episodes;')
        self.connection.action('DROP TABLE IF EXISTS tv_episodes;')
        self.connection.action('ALTER TABLE new_tv_episodes RENAME TO tv_episodes;')
        self.connection.action('DROP TABLE IF EXISTS new_tv_episodoes;')
        self.inc_minor_version()
