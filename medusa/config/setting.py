# coding=utf-8

import logging

from medusa import app, common, helpers, logger
from medusa.helpers.utils import split_and_strip
from medusa.logger.adapters.style import BraceAdapter

from six import iteritems, string_types

log = BraceAdapter(logging.getLogger(__name__))
log.logger.addHandler(logging.NullHandler())


def check_int(config, cfg_name, item_name, def_val, silent=True):
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


def check_bool(config, cfg_name, item_name, def_val, silent=True):
    return bool(check_int(config=config, cfg_name=cfg_name, item_name=item_name, def_val=def_val, silent=silent))


def check_float(config, cfg_name, item_name, def_val, silent=True):
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


def check_str(config, cfg_name, item_name, def_val, silent=True, censor_log=False, valid_values=None):
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


def check_list(config, cfg_name, item_name, default=None, silent=True, censor_log=False, transform=None, transform_default=0, split_value=False):
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


def check(config, section, attr_type, attr, default=None, silent=True, **kwargs):
    """
    Check setting from config file
    """
    func = {
        'string': check_str,
        'int': check_int,
        'float': check_float,
        'bool': check_bool,
        'list': check_list,
    }
    return func[attr_type](config, section, attr, default, silent, **kwargs)
