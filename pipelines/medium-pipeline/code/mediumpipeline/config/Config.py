from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, fabricName: str=None, date_from: str=None, date_to: str=None):
        self.spark = None
        self.update(fabricName, date_from, date_to)

    def update(self, fabricName: str, date_from: str, date_to: str):
        self.fabricName = fabricName
        self.date_from = date_from
        self.date_to = date_to
