import unittest
from datajob.etl.extract.news_extract import NewsExtractor


class MTest(unittest.TestCase):

    def test1(self):
        NewsExtractor.extract_data()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()

