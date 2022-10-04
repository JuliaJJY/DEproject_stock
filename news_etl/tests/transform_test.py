import unittest
from datajob.etl.extract.news_extract import NewsExtractor
from datajob.etl.transform.news_transform import NewsTransformer



class MTest(unittest.TestCase):

    def test1(self):
        NewsTransformer.transform()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()