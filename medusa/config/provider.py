# coding=utf-8

from medusa.config.setting import check as check_setting


def check(config, provider, attr_type, attr,
          default=None, silent=True, **kwargs):
    """Check provider setting from config file."""
    name = provider.get_id()
    section = name.upper()
    attr = '{name}_{attr}'.format(name=name, attr=attr)
    return check_setting(config, section, attr_type, attr, default, silent,
                         **kwargs)


def load(config, provider, attr_type, attr,
         default=None, silent=True, **kwargs):
    """Load provider setting from config file."""
    if hasattr(provider, attr):
        value = check(config, provider, attr_type, attr, default, silent,
                      **kwargs)
        setattr(provider, attr, value)


def save(config, provider, attr, **kwargs):
    """Save provider setting to config file."""
    if hasattr(provider, attr):
        section = kwargs.pop('section', provider.get_id().upper())
        setting = '{name}_{attr}'.format(name=provider.get_id(), attr=attr)
        value = kwargs.pop('value', getattr(provider, attr))
        if value in [True, False]:
            value = int(value)
        config[section][setting] = value
