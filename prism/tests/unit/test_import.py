"""
Unit testing to ensure that project can be imported

Table of Contents:
1: Test case class definition
"""

###################################
## 1: Test case class definition ##
###################################

import unittest
class TestImport(unittest.TestCase):
    
    def test_import_prism_main(self):
        """
        Project can be imported
        """
        import prism.main


# EOF