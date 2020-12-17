#!/bin/env python3

import argparse
import json
import random
import re
import sys
import itertools
import codecs
from enum import Enum
from getpass import getpass
import ssl
from json import JSONDecodeError
from typing import Optional
import cassandra
import cassandra.concurrent
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import cassandra.policies
import cassandra.query
import progressbar
import logging

TIMEOUT = 120.0
FETCH_SIZE = 100
CONCURRENT_BATCH_SIZE = 1000
UNSPECIFIED_PASSWORD = "___UNSPECIFIED_PASSWORD____"

args = None  # type: Optional[argparse.Namespace]
log = None  # type: Optional[logging.Logger]


class LineCountProgressBar(progressbar.ProgressBar):
    """
    A line counting progressbar, wrapped into a context manager.
    """
    SPINNERS = ('←↖↑↗→↘↓↙',
                '◢◣◤◥',
                '◰◳◲◱',
                '◴◷◶◵',
                '◐◓◑◒',
                "⠁⠂⠄⡀⢀⠠⠐⠈")

    def __init__(self) -> None:
        # Pick a random spinner
        markers = random.choice(self.SPINNERS)
        progressbar.ProgressBar.__init__(
            self,
            widgets=[progressbar.AnimatedMarker(markers=markers), progressbar.FormatLabel(" %(elapsed)s %(value)d rows")],
            maxval=progressbar.UnknownLength,
            fd=sys.stderr
        )

        # How often should the progress bar be updated (every X increments)
        self.update_interval = 1000


def cql_type(val):
    try:
        return val.data_type.typename
    except AttributeError:
        return val.cql_type


def format_cql_table_name(keyspace: str, table_name: str) -> str:
    """
    Returns the table name to use in CQL insert/update/truncate statements.

    "keyspace"."table"  # If not args.no_keyspace_name
    "table"             # If args.no_keyspace_name
    """
    if args.no_keyspace_name:
        fmt = '"{table}"'
    else:
        fmt = '"{keyspace}"."{table}"'

    return fmt.format(keyspace=keyspace, table=table_name)


def table_to_cqlfile(session, keyspace, tablename, flt, tableval, filep, limit=0):
    if flt is None:
        query = 'SELECT * FROM "' + keyspace + '"."' + tablename + '"'
    else:
        query = 'SELECT * FROM ' + flt

    if limit > 0:
        query = query + " LIMIT " + str(limit)

    rows = session.execute(query)

    def make_non_null_value_encoder(typename):
        if typename == 'blob':
            return session.encoder.cql_encode_bytes
        elif typename.startswith('map'):
            return session.encoder.cql_encode_map_collection
        elif typename.startswith('set'):
            return session.encoder.cql_encode_set_collection
        elif typename.startswith('list'):
            return session.encoder.cql_encode_list_collection
        else:
            return session.encoder.cql_encode_all_types

    def make_value_encoder(typename):
        e = make_non_null_value_encoder(typename)
        return lambda v: session.encoder.cql_encode_all_types(v) if v is None else e(v)

    def make_value_encoders(tabval):
        return {k: make_value_encoder(cql_type(v)) for k, v in tabval.columns.items()}

    def make_row_encoder():
        partitions = dict(
            (has_counter, list(k for k, v in columns))
            for has_counter, columns in itertools.groupby(tableval.columns.items(), lambda col: cql_type(col[1]) == 'counter')
        )

        # "keyspace"."table" or "table" if args.no_keyspace_name
        cql_table_name = format_cql_table_name(keyspace, tablename)

        counters = partitions.get(True, [])
        non_counters = partitions.get(False, [])
        columns = counters + non_counters

        if len(counters) > 0:
            def counter_row_encoder(vals):
                set_clause = ", ".join('%s = %s + %s' % (c, c, vals[c]) for c in counters if vals[c] != 'NULL')
                where_clause = " AND ".join('%s = %s' % (c, vals[c]) for c in non_counters)

                return 'UPDATE {table} SET {set_clause} WHERE {where_clause}'.format(
                    table=cql_table_name,
                    where_clause=where_clause,
                    set_clause=set_clause)

            return counter_row_encoder
        else:
            columns = list(counters + non_counters)

            def regular_row_encoder(vals):
                return 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
                    table=cql_table_name,
                    columns=', '.join('"{}"'.format(c) for c in columns if vals[c] != "NULL"),
                    values=', '.join(vals[c] for c in columns if vals[c] != "NULL"),
                )

        return regular_row_encoder

    value_encoders = make_value_encoders(tableval)
    row_encoder = make_row_encoder()

    with CommandHandler(log_=log) as cmd_handler:
        for i, row in enumerate(rows):
            values = dict((k, value_encoders[k](v)) for k, v in row.items())
            filep.write("%s;\n" % row_encoder(values))
            cmd_handler.update_line_count(i)


def can_execute_concurrently(statement):
    if args.sync:
        return False

    return statement.upper().startswith('INSERT') or statement.upper().startswith('UPDATE')


class ProgressBarType(Enum):
    LINE_COUNT = 'lc'
    """ Built-in twiddler progress bar based on line count """

    USER_DEFINED = 'cb'
    """ User defined progress bar, initialized/updated via comments in the CQL."""

    NONE = 'X'
    """ No progress bar in use"""

    PENDING = 'p'
    """ No progress bar in use; at first CQL line will be initialized as LINE_COUNT. """


class Command(Enum):
    """
    Commands that may be embedded into the CQL comments.  See CommandHandler
    """
    WRITE_LOG = 'CDUMP_LOG'
    PROGRESS_START = 'CDUMP_PROGRESS_START'
    PROGRESS_UPDATE = 'CDUMP_PROGRESS_UPDATE'
    PROGRESS_END = 'CDUMP_PROGRESS_END'


class LogLevel(Enum):
    """
    Unfortunately logging doesn't have a documented way to transform a log level string to the int in expects. :-(
    """
    FATAL = 'fatal'
    CRITICAL = 'critical'
    ERROR = 'error'
    WARNING = 'warning'
    INFO = 'info'
    DEBUG = 'debug'


class CommandHandler:
    """
    Processes special commands found in CQL comments and takes action.

    The expected format in the CQL is:
    # <COMMAND_NAME>: <JSON_DICT_PARAMS>

    Valid commands:
    # CDUMP_LOG: {"level": "info", "message": "Some log message"}
    -- Prints a message to the log (level: debug, info, warning, critical, fatal)

    # CDUMP_PROGRESS_START: {"total": 571790}
    -- Creates new user defined progress bar; disables the current progress bar.  Once created,
        the progress bar will be only be updated by CDUMP_PROGRESS_UPDATE.

    # CDUMP_PROGRESS_UPDATE: {"value": 568850}
    -- Update the user defined progress bar.

    # CDUMP_PROGRESS_END: {}
    -- Kill off the user defined progressbar
    """
    def __init__(self, log_: 'logging.Logger'):
        self.progress_bar = None
        self.progress_bar_type = ProgressBarType.PENDING
        self.log = log_

    def __enter__(self) -> 'CommandHandler':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_progress_bar()

    def handle_comment(self, line: str) -> None:
        """
        Processes a CQL comment and takes action.

        Comments that aren't valid commands are silently ignored.
        """
        m = re.match(r'# (?P<command>CDUMP_\w+):\S*(?P<params_json>.*)$', line)
        if not m:
            return

        try:
            cmd = Command(m.group('command'))
        except ValueError:
            # Invalid command
            return

        try:
            params = json.loads(m.group('params_json'))
        except JSONDecodeError:
            # Invalid json
            return

        if cmd is Command.WRITE_LOG:
            try:
                level = LogLevel(params.get('level'))
            except ValueError:
                level = LogLevel.INFO

            self.log.log(level=getattr(logging, level.name), msg=params['message'])

        elif cmd is Command.PROGRESS_START:
            self.set_user_progress_bar(total=params['total'])

        elif cmd is Command.PROGRESS_UPDATE:
            if self.progress_bar_type is ProgressBarType.USER_DEFINED:
                # Don't call update() with a value > max -- it will raise a ValueError.
                self.progress_bar.update(min(self.progress_bar.maxval, params['value']))

        elif cmd is Command.PROGRESS_END:
            self.end_progress_bar()

    def update_line_count(self, line_count: int) -> None:
        """
        Called as new lines are processed during import, updates the twiddler if it's currently displayed.
        """
        if self.progress_bar_type is ProgressBarType.LINE_COUNT:
            pass
        elif self.progress_bar_type is ProgressBarType.PENDING:
            # First CQL has been called & no user defined progress bar.  Setup the twiddler.
            self.progress_bar = LineCountProgressBar().start()
            self.progress_bar_type = ProgressBarType.LINE_COUNT
        else:
            # Twiddler not displayed
            return

        self.progress_bar.update(line_count)

    def set_user_progress_bar(self, total: int) -> None:
        """
        Initializes a new user-defined progress bar
        :param total: Maximum progress value
        """
        # Kill off any previous progress bar
        self.end_progress_bar()
        self.progress_bar = progressbar.ProgressBar(
            widgets=[progressbar.Percentage(), progressbar.Bar(), progressbar.ETA()], maxval=total, fd=sys.stderr
        ).start()
        self.progress_bar_type = ProgressBarType.USER_DEFINED

    def end_progress_bar(self) -> None:
        """
        Shutdown the active progress bar, if any.
        """
        if self.progress_bar is None:
            return

        self.progress_bar.finish()
        self.progress_bar = None
        self.progress_bar_type = ProgressBarType.NONE


def import_data(session):
    def enqueue_concurrent(statement_cql: str):
        """
        Add a concurrent statement to the queue.  Run the execution if we've got more than CONCURRENT_BATCH_SIZE statements.
        """
        concurrent_statements.append((statement_cql, None))
        if len(concurrent_statements) >= CONCURRENT_BATCH_SIZE:
            flush_concurrent()

    def flush_concurrent():
        if concurrent_statements:
            # Run tuple to force the generator to generate all rows.
            tuple(cassandra.concurrent.execute_concurrent(session, concurrent_statements))
            concurrent_statements.clear()

    if args.keyspace:
        session.set_keyspace(args.keyspace)
        assert session.keyspace == args.keyspace

    if args.file:
        fp = codecs.open(args.file, 'r', encoding='utf-8')
    else:
        fp = sys.stdin

    statement = ''
    concurrent_statements = []

    with CommandHandler(log_=log) as cmd_handler:
        for i, line in enumerate(fp):
            # Skip comments
            if line.startswith('#'):
                cmd_handler.handle_comment(line)
                continue

            cmd_handler.update_line_count(i)
            statement += line

            if statement.endswith(";\n"):
                if can_execute_concurrently(statement):
                    enqueue_concurrent(statement)
                else:
                    flush_concurrent()
                    session.execute(statement)

                statement = ''

    flush_concurrent()

    if statement:
        session.execute(statement)

    fp.close()


def get_keyspace_or_fail(session, keyname):
    keyspace = session.cluster.metadata.keyspaces.get(keyname)

    if not keyspace:
        sys.stderr.write('Can\'t find keyspace "' + keyname + '"\n')
        sys.exit(1)

    return keyspace


def get_column_family_or_fail(keyspace, tablename):
    tableval = keyspace.tables.get(tablename)

    if not tableval:
        sys.stderr.write('Can\'t find table "' + tablename + '"\n')
        sys.exit(1)

    return tableval


def log_cql(message: str, level: Optional[LogLevel] = LogLevel.INFO) -> str:
    """
    Returns the command string needed in order to log the specified message when the export is being imported.

    Example:
    log_cql(message="Hello", level=LogLevel.DEBUG)
    --> "# CDUMP_LOG: {"message": "Hello", "level": "debug"}\n"
    """
    params = {
        'level': level.value,
        'message': message
    }
    return "# CDUMP_LOG: {}\n".format(json.dumps(params))


def export_data(session):
    selection_options_count = sum(1 if x else 0 for x in (args.keyspace, args.cf, args.filter))
    assert selection_options_count <= 1, "--cf, --keyspace and --filter can\'t be combined"

    if args.file:
        f = codecs.open(args.file, 'w', encoding='utf-8')
    else:
        f = sys.stdout

    keyspaces = None
    exclude_list = []

    if selection_options_count == 0:
        log.info('Exporting all keyspaces')
        keyspaces = []
        for keyspace in session.cluster.metadata.keyspaces.keys():
            if keyspace not in ('system', 'system_traces'):
                keyspaces.append(keyspace)

    if args.limit is not None:
        limit = int(args.limit)
    else:
        limit = 0

    if args.keyspace is not None:
        keyspaces = args.keyspace
        if args.exclude_cf is not None:
            exclude_list.extend(args.exclude_cf)

    if keyspaces is not None:
        for keyname in keyspaces:
            keyspace = get_keyspace_or_fail(session, keyname)

            if not args.no_create:
                log.info('Exporting schema for keyspace ' + keyname)
                f.write(log_cql("Dropping keyspace {}".format(keyname)))
                f.write('DROP KEYSPACE IF EXISTS "' + keyname + '";\n')
                f.write(log_cql("Creating keyspace {}".format(keyname)))
                f.write(keyspace.export_as_string() + '\n')

            for tablename, tableval in keyspace.tables.items():
                if tablename in exclude_list:
                    log.info('Skipping data export for table ' + keyname + '.' + tablename)
                    continue
                elif tableval.is_cql_compatible:
                    tab_name = format_cql_table_name(keyname, tablename)
                    if args.truncate:
                        f.write(log_cql("Truncating {}".format(tab_name)))
                        f.write('TRUNCATE TABLE {table};\n'.format(table=tab_name))

                    if not args.no_insert:
                        log.info('Exporting data for table ' + keyname + '.' + tablename)
                        f.write(log_cql("Importing {}".format(tab_name)))
                        table_to_cqlfile(session, keyname, tablename, None, tableval, f, limit)

    if args.cf is not None:
        for cf in args.cf:
            if '.' not in cf:
                sys.stderr.write('Invalid keyspace.column_family input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]

            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if tableval.is_cql_compatible:
                if not args.no_create:
                    log.info('Exporting schema for table ' + keyname + '.' + tablename)
                    f.write(log_cql("Dropping {}.{}".format(keyname, tablename)))
                    f.write('DROP TABLE IF EXISTS "' + keyname + '"."' + tablename + '";\n')
                    f.write(log_cql("Creating {}.{}".format(keyname, tablename)))
                    f.write(tableval.export_as_string() + ';\n')

                if args.truncate:
                    f.write(log_cql("Truncating {}".format(tablename)))
                    f.write('TRUNCATE TABLE {table};\n'.format(table=format_cql_table_name(keyname, tablename)))

                if not args.no_insert:
                    log.info('Exporting data for table ' + keyname + '.' + tablename)
                    f.write(log_cql("Importing {}".format(tablename)))
                    table_to_cqlfile(session, keyname, tablename, None, tableval, f, limit)

    if args.filter is not None:
        for flt in args.filter:
            stripped = flt.strip()
            cf = stripped.split(' ')[0]

            if '.' not in cf:
                sys.stderr.write('Invalid input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]

            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if not tableval:
                sys.stderr.write('Can\'t find table "' + tablename + '"')
                sys.exit(1)

            if not args.no_insert:
                log.info('Exporting data for filter "' + stripped + '"')
                table_to_cqlfile(session, keyname, tablename, stripped, tableval, f, limit)

    f.close()


def get_credentials():
    return {'username': args.username, 'password': args.password}


def setup_cluster():
    if args.host is None:
        nodes = ['localhost']
    else:
        nodes = [args.host]

    if args.port is None:
        port = 9042
    else:
        port = int(args.port)

    if args.connect_timeout is None:
        connect_timeout = 5
    else:
        connect_timeout = int(args.connect_timeout)

    if args.ssl is not None and args.certfile is not None:
        ssl_opts = {'ca_certs': args.certfile,
                    'ssl_version': ssl.PROTOCOL_TLSv1,
                    'keyfile': args.userkey,
                    'certfile': args.usercert}
    else:
        ssl_opts = {}

    other_kwargs = {}
    if args.protocol_version:
        other_kwargs['protocol_version'] = args.protocol_version

    if args.username is not None and args.password is not None:
        if args.protocol_version and args.protocol_version == 1:
            other_kwargs['auth_provider'] = get_credentials
        else:
            other_kwargs['auth_provider'] = PlainTextAuthProvider(username=args.username, password=args.password)

    cluster = Cluster(control_connection_timeout=connect_timeout, connect_timeout=connect_timeout, contact_points=nodes, port=port,
                      load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(nodes), ssl_options=ssl_opts,
                      **other_kwargs)

    session = cluster.connect()

    session.default_timeout = TIMEOUT
    session.default_fetch_size = FETCH_SIZE
    session.row_factory = cassandra.query.ordered_dict_factory
    return session


def cleanup_cluster(session):
    session.cluster.shutdown()
    session.shutdown()


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """
    Add arguments to the arg parser which are common to both load & dump (ex: connection options)
    """
    parser.add_argument('--help', action='help', help="show this help message and exit")
    parser.add_argument('--connect-timeout', help='set timeout for connecting to the cluster (in seconds)', type=int)
    parser.add_argument('--host', '-h', help='the address of a Cassandra node in the cluster (localhost if omitted)')
    parser.add_argument('--port', help='the port of a Cassandra node in the cluster (9042 if omitted)')
    parser.add_argument('--protocol-version', help='set protocol version (required for C* 1.x)', type=int)
    parser.add_argument('--username', '-u', help='set username for auth')
    parser.add_argument('--password', '-p', help='set password for authentication', const=UNSPECIFIED_PASSWORD, nargs='?')
    parser.add_argument('--ssl', help='enable ssl connection to Cassandra cluster.  Must also set --certfile.', action='store_true')
    parser.add_argument('--certfile', help='ca cert file for SSL.  Assumes --ssl.')
    parser.add_argument('--userkey', help='user key file for client authentication.  Assumes --ssl.')
    parser.add_argument('--usercert', help='user cert file for client authentication.  Assumes --ssl.')
    parser.add_argument('--quiet', help='quiet progress logging', action='store_true')


def main():
    global args, log
    pparser = argparse.ArgumentParser(description='A data exporting tool for Cassandra inspired from mysqldump, with some added slice and dice capabilities.',
                                      add_help=False)

    # We add our own "--help" argument so that we can steal "-h" to be an alias for "--host".
    pparser.add_argument('--help', action='help', help="show this help message and exit")

    subparsers = pparser.add_subparsers(dest='command', help='actions')
    sp = subparsers.add_parser('export', help="Export data from Cassandra", add_help=False)  # type: argparse.ArgumentParser

    # Export args
    add_common_args(sp)
    sp.add_argument('--cf',
                    help='export a column family. The name must include the keyspace, e.g. "system.schema_columns". Can be specified multiple times',
                    nargs='+')
    sp.add_argument('--file', '-f', help='export data to the specified file, instead of stdout')
    sp.add_argument('--filter', help='export a slice of a column family according to a CQL filter. This takes essentially a typical SELECT query stripped '
                                     'of the initial "SELECT ... FROM" part (e.g. "system.schema_columns where keyspace_name =\'OpsCenter\'", and exports '
                                     'only that data. Can be specified multiple times', action='append')
    sp.add_argument('--keyspace', '-k', help='export a keyspace along with all its column families. Can be specified multiple times', action='append')
    sp.add_argument('--exclude-cf', help='when using --keyspace, specify column family to exclude.  Can be specified multiple times', action='append')
    sp.add_argument('--no-create', help="don't generate create (and drop) statements", action='store_true')
    sp.add_argument('--no-keyspace-name', help="don't add the keyspace name to insert statements.  Useful for loading data into a different keyspace.",
                    action='store_true')
    sp.add_argument('--no-insert', help="don't generate insert statements", action='store_true')
    sp.add_argument('--truncate', help="Add TRUNCATE <table> statement before each table's INSERT statements.", action='store_true')
    sp.add_argument('--relative', '-r',
                    help="Writes CQL such that the keyspace name is never referenced.  Useful for copying data from one keyspace "
                         "into another.  Equivalent to '--no-create --no-keyspace-name --truncate'",
                    action='store_true')
    sp.add_argument('--limit', help='set number of rows return limit')

    # Import args
    sp = subparsers.add_parser('import', help="Import data into Cassandra", add_help=False)  # type: argparse.ArgumentParser
    add_common_args(sp)
    sp.add_argument('--file', '-f', help='import data from the specified file, instead of stdin')
    sp.add_argument('--sync', help='import data in synchronous mode (default asynchronous)', action='store_true')
    sp.add_argument('--keyspace', '-k', help="Keyspace to switch to before executing CQL")

    args = pparser.parse_args()

    if not args.command:
        pparser.print_help()
        exit(2)

    if any((args.userkey, args.usercert)) and not all((args.userkey, args.usercert)):
        sys.stderr.write('--userkey and --usercert must both be provided\n')
        sys.exit(1)

    if args.ssl and not args.certfile:
        sys.stderr.write('--certfile must also be specified when using --ssl\n')
        sys.exit(1)

    if args.password and args.password == UNSPECIFIED_PASSWORD:
        # User specified "-p" but with no argument.  Prompt for the password.
        args.password = getpass()
    session = setup_cluster()

    # Init logging after Cassandra connection -- it logs to INFO on connect.
    if args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    # noinspection PyArgumentList
    logging.basicConfig(format='{asctime} ({process} {name}) [{levelname}] {message}', style='{', level=log_level)
    log = logging.getLogger()

    if args.command == 'import':
        import_data(session)
    elif args.command == 'export':
        if args.relative:
            args.no_create = True
            args.no_keyspace_name = True
            args.truncate = True

        if args.no_keyspace_name and not args.no_create:
            raise AssertionError("--no-keyspace-name can only be used with --no-create")
        export_data(session)
    else:
        raise AssertionError("Invalid command specified: {}".format(args.command))

    cleanup_cluster(session)


if __name__ == '__main__':
    main()
