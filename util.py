# -*- coding: UTF-8 -*-

import conf

def get_conf(item, default=None):
    """
    This is a simple isolation from conf module, this allows us to reload the
    conf assuming that there are no symbols imported on the current model.
    """

    return getattr(conf, item, default)

def reload_conf():
    """
    As we are avoiding import conf model, we encapsulate this.
    """

    reload(conf)
