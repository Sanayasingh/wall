def create_blockchain_wallet(
            self, wei_target_balance=2e16, wei_topup_threshold=1e16, private_key=None, queue='high-priority'
    ):
        """
        Creates a blockchain wallet on the blockchain worker
        :param wei_target_balance: How much eth to top the wallet's balance up to
        :param wei_topup_threshold: How low the wallet's balance should drop before attempting a topup
        :param private_key:
        :return: The wallet's address
        """
        args={
            'wei_target_balance': wei_target_balance,
            'wei_topup_threshold': wei_topup_threshold,
            'private_key': private_key
        }
        wallet_address = self._execute_synchronous_celery(
            self._eth_endpoint('create_new_blockchain_wallet'), args, queue=queue
        )

        if wei_target_balance or 0 > 0:
            self.topup_wallet_if_required(wallet_address, queue=queue)

        return wallet_address 