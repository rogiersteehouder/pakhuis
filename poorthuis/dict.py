class DictAccounts:
    """Accounts in a dictionary"""

    def __init__(self, accounts: dict = None):
        self.accounts = {}
        if accounts is not None:
            self.accounts.update(accounts)

    def __call__(self, username: str) -> str:
        """Password hash for user"""
        return self.accounts.get(username)
