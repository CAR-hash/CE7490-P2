import json


class ConfigObject(object):
    def __init__(self, config):
        config.replace("\n", "")
        self.config_str = config
        self.dic = json.loads(config)
        for key in self.dic.keys():
            self.__setattr__(key, self.dic[key])