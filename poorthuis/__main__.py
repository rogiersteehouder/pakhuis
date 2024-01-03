import getpass

from . import crypt_context

user = input("Username: ")
pwd = getpass.getpass("Password: ")
print("Add this to your config:")
print('"{}" = "{}"'.format(user.lower(), crypt_context.hash(pwd)))
