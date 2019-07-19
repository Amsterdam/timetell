from django.test import TestCase
from ois_projects_employees.api.models import Emp


class BooleanField(TestCase):

    def test_boolean_field_false(self):
        """Boolean field type is correctly parsed"""
        emp = Emp(nonactive=0)
        self.assertEqual(emp.nonactive, False,
                         "expected boolean False, got {emp.nonactive}")

    def test_boolean_field_true(self):
        """Boolean field type is correctly parsed"""
        emp = Emp(nonactive=int(1))
        self.assertEqual(emp.nonactive, True,
                         "expected boolean True, instead get {emp.nonactive}")
