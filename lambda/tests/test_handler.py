import unittest
from etl.extract import extract
from etl.transform import transform
from etl.load import load

# First 'user' record generated from default seed 56d27f4a53bd5441:
#    "gender": "female",
#    "login": { ... "username": "yellowpeacock117" ... }
#    "location": { ... "country": "Canada" ... },
#    "email": "eva.martin@example.com" 

class Extract_TestCase(unittest.TestCase):
    def test_returnsList(self):
        self.assertIsInstance(extract(), list)

    def test_limitX_returnsXRecord(self):
        self.assertEqual(len(extract()), 1)
        self.assertEqual(len(extract(limit=2)), 2)
        self.assertEqual(len(extract(limit=5000)), 5000)

    def test_returnsListItemsWithProperties(self):
        user: dict = extract()[0]
        self.assertIn('gender', user)
        self.assertIn('login', user)
        self.assertIn('username', user['login'])
        self.assertIn('location', user)
        self.assertIn('email', user)

    def test_returnsListItemsWithValues(self):
        user: dict = extract(seed="56d27f4a53bd5441")[0]
        self.assertEqual(user['gender'], 'female')
        self.assertEqual(user['login']['username'], 'yellowpeacock117')
        self.assertEqual(user['location']['country'], 'Canada')
        self.assertEqual(user['email'], 'eva.martin@example.com')

class Transform_TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.users = extract(seed="56d27f4a53bd5441")
        self.countries = transform(self.users)

    def test_returnsList(self):
        self.assertIsInstance(self.countries, list)

    def test_returnsListItemsOfTypeDict(self):
        self.assertIsInstance(self.countries[0], dict)

    def test_returnsListItemsWithProperties(self):
        self.assertIn('country', self.countries[0])
        self.assertIn('users', self.countries[0])
        self.assertIn('name', self.countries[0]['users'][0])
        self.assertIn('gender', self.countries[0]['users'][0])
        self.assertIn('email', self.countries[0]['users'][0])

    def test_userFromCountryX_returnsInCountryX(self):
        self.assertEqual(self.countries[0]['country'], 'Canada')
        self.assertEqual(self.countries[0]['users'][0]['name'], 'yellowpeacock117')
        self.assertEqual(self.countries[0]['users'][0]['email'], 'eva.martin@example.com')
        self.assertEqual(self.countries[0]['users'][0]['gender'], 'female')

class Load_TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.users = extract(limit=5, seed="56d27f4a53bd5441")
        self.countries = transform(self.users)

    def test_nothing(self):
        load(self.countries)

        