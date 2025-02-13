import configparser


class Config:
    __config = None

    @classmethod
    def __init(cls):
        if not cls.__config:
            cls.__config = configparser.ConfigParser()
            cls.__config.read("config.ini", encoding="utf-8")
            print("config loaded.")

    @classmethod
    def order(cls):
        cls.__init()
        return cls.__config["order"]

    @classmethod
    def redis(cls):
        cls.__init()
        return cls.__config["redis"]

    @classmethod
    def json(cls):
        cls.__init()
        return cls.__config["json"]

    @classmethod
    def api(cls):
        cls.__init()
        return cls.__config["api"]

    @classmethod
    def slack(cls):
        cls.__init()
        return cls.__config["slack"]
